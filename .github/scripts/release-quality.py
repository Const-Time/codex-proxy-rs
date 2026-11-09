"""Reuse quality only for version-only releases with complete CI on their parent."""
import json
import os
import subprocess
import urllib.request

ALLOWED = {"release/version.yaml", "release/notes.md", "release/channels.json"}
REQUIRED = ("quality / backend", "quality / frontend", "quality / workflow-lint")

def reusable(changed, run, jobs, parent):
    return (bool(changed) and set(changed) <= ALLOWED
            and run.get("event") == "push" and run.get("head_branch") == "main"
            and run.get("head_sha") == parent and run.get("conclusion") == "success"
            and all(any(job.get("name") == name and job.get("conclusion") == "success"
                        for job in jobs) for name in REQUIRED))

def main():
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    parent = sha
    changed = []
    for _ in range(10):
        candidate = subprocess.check_output(["git", "rev-parse", f"{parent}^"], text=True).strip()
        delta = subprocess.check_output(["git", "diff", "--name-only", candidate, parent], text=True).splitlines()
        if not delta or not set(delta) <= ALLOWED:
            break
        changed.extend(delta)
        parent = candidate
    if not changed:
        return False
    repo = os.environ["GITHUB_REPOSITORY"]
    def api(path):
        request = urllib.request.Request(f"https://api.github.com/repos/{repo}/{path}", headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json", "User-Agent": "cpr-release-quality"})
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)
    try:
        runs = api(f"actions/workflows/ci.yml/runs?head_sha={parent}&event=push&status=success&per_page=5")["workflow_runs"]
        for run in runs:
            jobs = api(f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100")["jobs"]
            if reusable(changed, run, jobs, parent):
                return True
    except (OSError, ValueError, KeyError):
        # Missing evidence or transient API failures always run the full checks.
        return False
    return False

if __name__ == "__main__":
    print("reuse_backend=" + str(main()).lower())
