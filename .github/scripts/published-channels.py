"""Advance a bounded published index only after every versioned asset is uploaded."""
import base64
import json
import os
import re
import subprocess
from urllib.parse import quote

PATH = "release/channels.json"

def identity(raw):
    match = re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(alpha|beta|rc|exp)\.([1-9][0-9]*))?", raw)
    if not match:
        raise ValueError("unsupported published version")
    major, minor, patch = map(int, match.group(1, 2, 3))
    stage, sequence = match.group(4, 5)
    key = f"{major}.{minor}.{patch}/{stage or 'stable'}"
    return key, (major, minor, patch, int(sequence or 0))

def advance(document, version):
    if document.get("schema") != 1 or not isinstance(document.get("versions"), list):
        raise ValueError("invalid published index")
    latest = {}
    for value in [*document["versions"], version]:
        key, order = identity(value)
        if key not in latest or identity(latest[key])[1] < order:
            latest[key] = value
    prerelease_cores = {value.split("-", 1)[0] for value in latest.values() if "-" in value}
    stable_by_major = {}
    for value in latest.values():
        if "-" not in value:
            major = identity(value)[1][0]
            if major not in stable_by_major or identity(stable_by_major[major])[1] < identity(value)[1]:
                stable_by_major[major] = value
    values = sorted(value for value in latest.values() if "-" in value or value in prerelease_cores or value in stable_by_major.values())
    if len(values) > 1024:
        raise ValueError("published index exceeds limit")
    return {"schema": 1, "versions": values}

def verify_assets(release, version, platforms):
    identity(version)
    if release.get("tag_name") != "v" + version or release.get("draft") or release.get("prerelease") != ("-" in version):
        raise ValueError("release identity is not published")
    expected = {f"codex-proxy-rs_{version}_{p['os']}_{p['arch']}.{p['archive']}" for p in platforms}
    if not expected:
        raise ValueError("no release platforms")
    expected.update(["checksums.txt", "compose.yaml", "config.example.yaml"])
    present = {a["name"] for a in release.get("assets", []) if a.get("state") == "uploaded" and a.get("size", 0) > 0}
    if not expected <= present:
        raise ValueError("missing release assets: " + ", ".join(sorted(expected - present)))

def api(path, data=None):
    command = ["gh", "api", path]
    if data is not None:
        command += ["--method", "PUT", "--input", "-"]
    return json.loads(subprocess.check_output(command, input=json.dumps(data).encode() if data is not None else None))

def main():
    repo, tag = os.environ["GITHUB_REPOSITORY"], os.environ["RELEASE_TAG"]
    version = tag.removeprefix("v")
    base = f"repos/{repo}"
    release = api(f"{base}/releases/tags/{quote(tag, safe='')}")
    platform_file = api(f"{base}/contents/release/platforms.yaml?ref={quote(tag, safe='')}")
    # Ruby/YAML is already required by release metadata; read platforms from the immutable tag.
    platforms = json.loads(subprocess.check_output(["ruby", "-ryaml", "-rjson", "-e", "puts JSON.generate(YAML.safe_load(STDIN.read).fetch('platforms'))"], input=base64.b64decode(platform_file["content"])))
    verify_assets(release, version, platforms)
    current = api(f"{base}/contents/{PATH}?ref=main")
    document = json.loads(base64.b64decode(current["content"]))
    updated = advance(document, version)
    if updated == document:
        return
    content = (json.dumps(updated, indent=2) + "\n").encode()
    if len(content) > 128 * 1024:
        raise ValueError("published index exceeds byte limit")
    api(f"{base}/contents/{PATH}", {"branch": "main", "sha": current["sha"], "content": base64.b64encode(content).decode(), "message": f"chore(release): publish channel index for {tag}"})

if __name__ == "__main__":
    main()
