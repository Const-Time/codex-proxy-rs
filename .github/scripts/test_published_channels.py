import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("channels", Path(__file__).with_name("published-channels.py"))
channels = importlib.util.module_from_spec(spec)
spec.loader.exec_module(channels)

class ChannelsTest(unittest.TestCase):
    def test_late_releases_preserve_newer_and_other_channels(self):
        start = {"schema": 1, "versions": ["3.18.3", "4.0.0", "3.19.0-beta.2", "3.19.0-exp.4"]}
        updated = channels.advance(start, "3.18.2")
        self.assertEqual(set(updated["versions"]), set(start["versions"]))
        self.assertEqual(set(channels.advance(updated, "3.19.0-beta.3")["versions"]), {"3.18.3", "4.0.0", "3.19.0-beta.3", "3.19.0-exp.4"})
        with self.assertRaises(ValueError): channels.advance(start, "invalid")
    def test_stable_promotion_remains_available_to_older_prerelease_cycle(self):
        start = {"schema": 1, "versions": ["3.20.0", "3.19.0-rc.1"]}
        updated = channels.advance(start, "3.19.0")
        self.assertEqual(set(updated["versions"]), {"3.20.0", "3.19.0-rc.1", "3.19.0"})
    def test_publication_requires_all_uploaded_assets(self):
        platforms = [{"os":"linux", "arch":"amd64", "archive":"tar.gz"}]
        names = ["codex-proxy-rs_3.18.3_linux_amd64.tar.gz", "checksums.txt", "compose.yaml", "config.example.yaml"]
        release = {"tag_name":"v3.18.3", "draft":False, "prerelease":False, "assets":[{"name":name,"state":"uploaded","size":1} for name in names]}
        channels.verify_assets(release, "3.18.3", platforms)
        for change in [{"draft":True}, {"tag_name":"v3.18.2"}, {"prerelease":True}, {"assets":release["assets"][:-1]}]:
            with self.assertRaises(ValueError): channels.verify_assets(dict(release, **change), "3.18.3", platforms)

if __name__ == "__main__": unittest.main()
