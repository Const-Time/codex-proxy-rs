"""One-time, fail-closed withdrawal after the replacement v3.10.0 is published.

Uses the release workflow's existing repository/package permissions. A plan is
archived before apply; no branch history, platform images or live instances are
deleted. Re-runs tolerate already withdrawn objects but never changed targets.
"""

import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen


REPOSITORY = "Const-Time/codex-proxy-rs"
OLD_COMMIT = "762f71de1ee072dd294849f52181384e92597888"
OLD_TAG_OBJECT = "941f0c262009e4ebbe8e36518e1b5d74ff31979d"
OLD_RELEASE_ID = 391384868
OLD_IMAGE_DIGEST = "sha256:1adad2653d7f1db5ac98dac45c2eb32d97510b9ff8758c047b2c92913e44e401"
REPO_API = f"/repos/{REPOSITORY}"
PACKAGE_API = "/users/Const-Time/packages/container/codex-proxy-rs/versions"
ALLOWED_OLD_IMAGE_TAGS = {"4.0.0", f"sha-{OLD_COMMIT}"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def api(path, method="GET", missing_ok=False):
    request = Request(
        f"https://api.github.com{path}",
        method=method,
        headers={
            "Authorization": f"Bearer {os.environ['GH_TOKEN']}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "codex-proxy-release",
        },
    )
    try:
        with urlopen(request, timeout=60) as response:
            body = response.read()
            return json.loads(body) if body else None
    except HTTPError as error:
        if missing_ok and error.code == 404:
            return None
        # Do not print request headers or authentication material.
        raise RuntimeError(f"GitHub {method} {path}: HTTP {error.code}") from None


def tags(version):
    return set(version["metadata"]["container"]["tags"])


def plan():
    require(os.environ.get("GITHUB_REPOSITORY") == REPOSITORY, "Unexpected repository")
    require(os.environ.get("RELEASE_TAG") == "v3.10.0", "Only v3.10.0 may withdraw v4")
    replacement = api(f"{REPO_API}/releases/tags/v3.10.0")
    require(not replacement["draft"] and not replacement["prerelease"], "Replacement not stable")
    required_assets = {
        "checksums.txt",
        *(f"codex-proxy-rs_3.10.0_{p}.tar.gz"
          for p in ("linux_amd64", "linux_arm64", "darwin_arm64")),
    }
    require(
        required_assets <= {a["name"] for a in replacement["assets"] if a["size"] > 0},
        "Replacement assets incomplete",
    )
    require(api(f"{REPO_API}/releases/latest")["id"] == replacement["id"], "Latest not replacement")
    old_release = api(f"{REPO_API}/releases/tags/v4.0.0", missing_ok=True)
    if old_release:
        require(old_release["id"] == OLD_RELEASE_ID, "Old release identity changed")
    old_ref = api(f"{REPO_API}/git/ref/tags/v4.0.0", missing_ok=True)
    if old_ref:
        require(old_ref["object"]["sha"] == OLD_TAG_OBJECT, "Old tag changed")
        old_tag = api(f"{REPO_API}/git/tags/{OLD_TAG_OBJECT}")
        require(old_tag["object"]["sha"] == OLD_COMMIT, "Old tag commit changed")

    old_image = None
    replacement_image = None
    for page in range(1, 101):
        versions = api(f"{PACKAGE_API}?per_page=100&page={page}")
        for version in versions:
            if "4.0.0" in tags(version):
                require(old_image is None, "Duplicate old image tag")
                old_image = version
            if "3.10.0" in tags(version):
                require(replacement_image is None, "Duplicate replacement image tag")
                replacement_image = version
        if len(versions) < 100 or (old_image and replacement_image):
            break
    else:
        raise RuntimeError("Package listing incomplete")
    require(replacement_image is not None, "Replacement container missing")
    require("latest" in tags(replacement_image), "Container latest not replacement")
    require(
        replacement_image["name"] == os.environ.get("REPLACEMENT_DIGEST"),
        "Replacement container differs from verified build",
    )
    if old_image:
        require(old_image["name"] == OLD_IMAGE_DIGEST, "Old image digest changed")
        require(tags(old_image) <= ALLOWED_OLD_IMAGE_TAGS, "Old image has other retained tags")
        require(old_image["id"] != replacement_image["id"], "Old and replacement image identical")
    return {
        "replacement_release": replacement,
        "replacement_image": replacement_image,
        "old_release": old_release,
        "old_ref": old_ref,
        "old_image": old_image,
    }


def apply(saved):
    current = plan()
    for name in ("replacement_release", "replacement_image"):
        require(current[name]["id"] == saved[name]["id"], f"{name} changed since backup")
    for name in ("old_release", "old_ref", "old_image"):
        if current[name]:
            require(saved[name] is not None, f"{name} appeared since backup")
            key = "object" if name == "old_ref" else "id"
            require(current[name][key] == saved[name][key], f"{name} changed since backup")
    # Delete only the old multi-platform index, never the package or child
    # platform manifests. The replacement and latest were checked above.
    if current["old_image"]:
        api(f"{PACKAGE_API}/{current['old_image']['id']}", "DELETE")
    if current["old_release"]:
        api(f"{REPO_API}/releases/{OLD_RELEASE_ID}", "DELETE")
    if current["old_ref"]:
        api(f"{REPO_API}/git/refs/tags/v4.0.0", "DELETE")
    verified = plan()
    require(
        all(verified[name] is None for name in ("old_image", "old_release", "old_ref")),
        "Withdrawal verification failed",
    )
    print("v4.0.0 release, Git tag and 4.0.0 container index withdrawn; v3.10.0 remains latest")


if __name__ == "__main__":
    mode, path = sys.argv[1:]
    if mode == "plan":
        Path(path).write_text(json.dumps(plan(), indent=2), encoding="utf-8")
        print("Validated withdrawal targets and saved metadata backup")
    elif mode == "apply":
        apply(json.loads(Path(path).read_text(encoding="utf-8")))
    else:
        raise SystemExit("Expected plan or apply")
