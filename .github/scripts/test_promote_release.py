import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("promote", Path(__file__).with_name("promote-release.py"))
promote = importlib.util.module_from_spec(spec)
spec.loader.exec_module(promote)


def release(tag, **kwargs):
    return dict(id=tag, tag_name=tag, draft=False, prerelease=False, **kwargs)


class PromotionTest(unittest.TestCase):
    def test_out_of_order_runs_and_numeric_versions(self):
        releases = [release("v3.9.0"), release("v3.10.0")]
        self.assertIsNone(promote.candidate(releases, "v3.9.0"))
        self.assertEqual(promote.candidate(releases, "v3.10.0"), releases[1])

    def test_draft_prerelease_and_rerun(self):
        stable = release("v3.13.0")
        newer = release("v3.14.0")
        newer["draft"] = True
        self.assertEqual(promote.candidate([stable, newer, release("v4.0.0-beta.1")], "v3.13.0"), stable)
        with self.assertRaises(ValueError):
            promote.candidate([newer], "v3.14.0")


if __name__ == "__main__":
    unittest.main()
