"""Publish independent platform assets under the workflow's publication lock."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import quote


def checksums(text):
    entries = {}
    for line in text.splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.+-]+)", line)
        if not match or match[2] in entries:
            raise ValueError("invalid or duplicate release checksum")
        entries[match[2]] = match[1]
    return entries


def merge_checksums(previous, current):
    entries = checksums(previous)
    entries.update(checksums(current))
    return "".join(f"{digest}  {name}\n" for name, digest in sorted(entries.items()))


def release(repo, tag):
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}/releases/tags/{quote(tag, safe='')}"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode == 0:
        value = json.loads(result.stdout)
        if value.get("draft") or value.get("tag_name") != tag:
            raise ValueError("unexpected release identity")
        return value
    # Authentication, throttling and network failures must not masquerade as absence.
    if "HTTP 404" in result.stderr:
        return None
    raise RuntimeError(result.stderr)


def main():
    mode = sys.argv[1]
    if mode not in ("merge", "publish"):
        raise ValueError("unsupported release operation")
    repo, tag = os.environ["GITHUB_REPOSITORY"], os.environ["RELEASE_TAG"]
    directory = Path("dist/release")
    local = directory.joinpath("checksums.txt").read_text(encoding="utf-8")
    for name, digest in checksums(local).items():
        if hashlib.sha256(directory.joinpath(name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"local asset checksum mismatch: {name}")
    existing = release(repo, tag)
    previous = ""
    if existing:
        for asset in existing.get("assets", []):
            if asset["name"] == "checksums.txt":
                if asset.get("state") != "uploaded" or not 0 < asset["size"] <= 2 * 1024 * 1024:
                    raise ValueError("invalid published checksums asset")
                previous = subprocess.check_output([
                    "gh", "api", f"repos/{repo}/releases/assets/{asset['id']}",
                    "-H", "Accept: application/octet-stream",
                ]).decode("utf-8")
    directory.joinpath("checksums.txt").write_text(merge_checksums(previous, local), encoding="utf-8")
    if mode == "merge":
        return
    if existing is None:
        command = ["gh", "release", "create", tag, "--repo", repo, "--verify-tag",
                   "--title", tag, "--notes-file", "release/notes.md", "--latest=false"]
        if os.environ["PRERELEASE"] == "true":
            command.append("--prerelease")
        subprocess.run(command, check=True)
    # Upload checksums last so discovery cannot advertise an asset before it exists.
    assets = sorted(p for p in directory.iterdir() if p.name != "checksums.txt")
    for batch in [assets, [directory / "checksums.txt"]]:
        subprocess.run(["gh", "release", "upload", tag, "--repo", repo, "--clobber", *map(str, batch)], check=True)


if __name__ == "__main__":
    main()
