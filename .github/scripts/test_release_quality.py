import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("quality", Path(__file__).with_name("release-quality.py"))
quality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quality)

class QualityTest(unittest.TestCase):
    def test_requires_identical_source_and_complete_trusted_ci(self):
        run = dict(event="push", head_branch="main", head_sha="abc", conclusion="success")
        jobs = [dict(name=n, conclusion="success") for n in quality.REQUIRED]
        self.assertTrue(quality.reusable(["release/version.yaml", "release/notes.md"], run, jobs, "abc"))
        for files, changed_run, changed_jobs, parent in [
            ([], run, jobs, "abc"), (["backend/Cargo.lock"], run, jobs, "abc"),
            (["release/version.yaml"], dict(run, event="pull_request"), jobs, "abc"),
            (["release/version.yaml"], run, jobs, "other"),
            (["release/version.yaml"], dict(run, conclusion="failure"), jobs, "abc"),
            (["release/version.yaml"], run, jobs[:-1], "abc"),
            (["release/version.yaml"], run, [dict(j, conclusion="skipped") for j in jobs], "abc"),
        ]:
            self.assertFalse(quality.reusable(files, changed_run, changed_jobs, parent))

if __name__ == "__main__": unittest.main()
