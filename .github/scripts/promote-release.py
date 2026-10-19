"""Promote only the highest published stable version, inside the publish lock."""
import json
import os
import re
import subprocess


def stable_version(tag):
    match = re.fullmatch(r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag)
    return tuple(map(int, match.groups())) if match else None


def candidate(releases, tag):
    version = stable_version(tag)
    stable = [r for r in releases if not r["draft"] and not r["prerelease"] and stable_version(r["tag_name"]) is not None]
    target = next((r for r in stable if r["tag_name"] == tag), None)
    if target is None or version is None:
        raise ValueError("target is not a published stable release")
    return target if all(stable_version(r["tag_name"]) <= version for r in stable) else None


def main():
    if os.environ["PRERELEASE"] == "true":
        return
    repository = os.environ["GITHUB_REPOSITORY"]
    pages = json.loads(subprocess.check_output([
        "gh", "api", "--paginate", "--slurp", f"repos/{repository}/releases?per_page=100",
    ]))
    target = candidate([r for page in pages for r in page], os.environ["RELEASE_TAG"])
    if target is None:
        print("A newer stable release exists; keep its latest pointers.")
        return
    image, digest = os.environ["IMAGE"], os.environ["DIGEST"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("missing verified image digest")
    # The Release and all versioned assets already exist. A failure between these
    # operations cannot let an older run demote latest: it sees this Release too.
    subprocess.run(["docker", "buildx", "imagetools", "create", "-t", f"{image}:latest", f"{image}@{digest}"], check=True)
    subprocess.run(["gh", "api", "--method", "PATCH", f"repos/{repository}/releases/{target['id']}", "-f", "make_latest=true"], check=True, stdout=subprocess.DEVNULL)
    print(f"Promoted {target['tag_name']} to latest.")


if __name__ == "__main__":
    main()
