"""Only reuse verified artifacts from the requested release commit."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("repair", Path(__file__).with_name("repair-release.py"))
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


class RepairReleaseTest(unittest.TestCase):
    def test_source_run_must_match_tag_commit_and_pass_required_jobs(self):
        run = {"repository": {"full_name": "test/repo"}, "path": ".github/workflows/release.yml",
               "event": "push", "head_sha": "abc", "head_branch": "v1.2.3", "status": "completed"}
        jobs = [{"name": name, "conclusion": "success"} for name in [
            "release-info", "quality / frontend", "quality / workflow-lint", "publish", "build-binary (darwin-arm64)"]]
        platforms = [{"os": "darwin", "arch": "arm64"}]
        repair.verify(run, jobs, "test/repo", "v1.2.3", "abc", platforms)
        for key, value in [("head_sha", "wrong"), ("head_branch", "main"), ("event", "pull_request"),
                           ("status", "in_progress"), ("repository", {"full_name": "other/repo"}),
                           ("path", ".github/workflows/ci.yml")]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                repair.verify(dict(run, **{key: value}), jobs, "test/repo", "v1.2.3", "abc", platforms)
        for index in range(len(jobs)):
            changed = [dict(job) for job in jobs]
            changed[index]["conclusion"] = "failure"
            with self.subTest(job=index), self.assertRaises(ValueError):
                repair.verify(run, changed, "test/repo", "v1.2.3", "abc", platforms)


if __name__ == "__main__":
    unittest.main()
