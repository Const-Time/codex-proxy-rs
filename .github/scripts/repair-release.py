"""Verify a completed release run before reusing its non-Docker artifacts."""
import json
import os
from pathlib import Path
import re
import subprocess


def verify(run, jobs, repo, tag, sha, platforms):
    if (run.get("repository", {}).get("full_name") != repo
            or run.get("path") != ".github/workflows/release.yml"
            or run.get("event") not in ("push", "workflow_dispatch")
            or run.get("head_sha") != sha
            or run.get("head_branch") != tag
            or run.get("status") != "completed"):
        raise ValueError("source run does not match the completed tagged release")
    required = {"release-info", "quality / frontend", "quality / workflow-lint", "publish"}
    required.update(f"build-binary ({p['os']}-{p['arch']})" for p in platforms)
    successful = {job["name"] for job in jobs if job.get("conclusion") == "success"}
    if not required <= successful:
        raise ValueError("source run lacks successful checks/builds: " + ", ".join(sorted(required - successful)))
    # The successful publish job already depends on quality; backend can be
    # skipped only through the release workflow's verified reuse mechanism.


def main():
    repo, tag, run_id = (os.environ[key] for key in ("GITHUB_REPOSITORY", "RELEASE_TAG", "SOURCE_RUN_ID"))
    if not re.fullmatch(r"v\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?", tag) or not run_id.isdigit():
        raise ValueError("invalid tag or source run ID")
    sha = subprocess.check_output(["git", "-C", "source", "rev-parse", "HEAD"], text=True).strip()
    metadata = json.loads(subprocess.check_output([
        "ruby", "-ryaml", "-rjson", "-e",
        "puts JSON.generate({version: YAML.load_file('source/release/version.yaml').fetch('version'), platforms: YAML.load_file('source/release/platforms.yaml').fetch('platforms')})",
    ]))
    version = metadata["version"].removeprefix("v")
    if tag != "v" + version:
        raise ValueError("tag does not match release version")
    platforms = [p for p in metadata["platforms"] if not p.get("docker", False)]
    if not platforms or any(p["os"] != "darwin" for p in platforms):
        raise ValueError("repair currently supports Darwin release artifacts")
    base = f"repos/{repo}/actions/runs/{run_id}"
    run = json.loads(subprocess.check_output(["gh", "api", base]))
    pages = json.loads(subprocess.check_output(["gh", "api", "--paginate", "--slurp", base + "/jobs?filter=latest&per_page=100"]))
    verify(run, [job for page in pages for job in page["jobs"]], repo, tag, sha, platforms)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        for key, value in {
            "version": version, "docker_version": version,
            "image_name": repo.lower(), "matrix": json.dumps({"include": platforms}),
        }.items():
            output.write(f"{key}={value}\n")


if __name__ == "__main__":
    main()
