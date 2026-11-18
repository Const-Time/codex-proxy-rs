"""Keep independent platform uploads from losing another platform's checksums."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("assets", Path(__file__).with_name("release-assets.py"))
assets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assets)


class ReleaseAssetsTest(unittest.TestCase):
    def test_packaging_requires_only_its_own_platform(self):
        script = Path(__file__).with_name("package-release.py").resolve()
        for operating_system, flat in [("linux", False), ("darwin", False), ("darwin", True)]:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                def write(name, content):
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content)
                write("deploy/config.example.yaml", "server: {}\n")
                write("deploy/compose.yaml", "image: ${CPR_IMAGE:-ghcr.io/test/repo:latest}\n")
                write("dist/frontend/index.html", "<title>Fixture</title>")
                write("backend/apps/shared-budget-plugin/plugin.json", json.dumps({"publisher":"local","name":"budget","version":"1.0.0"}))
                base = "dist/binaries" if flat else f"dist/binaries/codex-proxy-rs-{operating_system}-arm64"
                write(base + "/codex-proxy-rs", "fixture-binary")
                write(base + "/cpr-plugin", "fixture-cli")
                write(base + "/plugins/local.budget-1.0.0-fixture-target.tar.gz", "fixture-plugin")
                platform = {"os":operating_system,"arch":"arm64","binary":"codex-proxy-rs","archive":"tar.gz","target":"fixture-target"}
                environment = dict(os.environ, VERSION="1.2.3", DOCKER_VERSION="1.2.3", REGISTRY="ghcr.io", IMAGE_NAME="test/repo", PLATFORM_MATRIX=json.dumps({"include":[platform]}))
                subprocess.run([sys.executable, str(script)], cwd=root, env=environment, check=True)
                entries = assets.checksums((root / "dist/release/checksums.txt").read_text())
                self.assertIn(f"codex-proxy-rs_1.2.3_{operating_system}_arm64.tar.gz", entries)
                self.assertEqual(len(entries), 4)
                for name, digest in entries.items():
                    self.assertEqual(assets.hashlib.sha256((root / "dist/release" / name).read_bytes()).hexdigest(), digest)
                if flat:
                    other = dict(platform, arch="amd64", target="other-target")
                    environment["PLATFORM_MATRIX"] = json.dumps({"include": [platform, other]})
                    result = subprocess.run([sys.executable, str(script)], cwd=root, env=environment, capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("Missing binary artifact", result.stderr)

    def test_both_completion_orders_preserve_all_platforms(self):
        linux = "a" * 64 + "  linux.tar.gz\n" + "b" * 64 + "  compose.yaml\n"
        mac = "c" * 64 + "  darwin.tar.gz\n" + "b" * 64 + "  compose.yaml\n"
        self.assertEqual(assets.merge_checksums(linux, mac), assets.merge_checksums(mac, linux))
        merged = assets.checksums(assets.merge_checksums(linux, mac))
        self.assertEqual(set(merged), {"linux.tar.gz", "darwin.tar.gz", "compose.yaml"})
        retried = assets.merge_checksums(assets.merge_checksums(linux, mac), "d" * 64 + "  darwin.tar.gz\n")
        self.assertEqual(assets.checksums(retried)["linux.tar.gz"], "a" * 64)
        self.assertEqual(assets.checksums(retried)["darwin.tar.gz"], "d" * 64)

    def test_malformed_or_traversal_names_fail(self):
        for text in ["bad", "a" * 64 + "  ../secret\n", ("a" * 64 + "  asset\n") * 2]:
            with self.assertRaises(ValueError):
                assets.checksums(text)

    def test_release_network_failure_is_not_absence(self):
        import subprocess
        with patch.object(assets.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", "HTTP 503")):
            with self.assertRaises(RuntimeError):
                assets.release("owner/repo", "v1.2.3")
        with patch.object(assets.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", "HTTP 404")):
            self.assertIsNone(assets.release("owner/repo", "v1.2.3"))

    def test_linux_and_docker_publication_do_not_depend_on_macos(self):
        # Inspect job dependency boundaries without requiring a third-party YAML parser.
        text = Path(__file__).parents[1].joinpath("workflows/release.yml").read_text()
        linux = text.split("  package-assets:\n", 1)[1].split("  package-non-docker-assets:\n", 1)[0]
        publish = text.split("  publish:\n", 1)[1].split("  publish-non-docker:\n", 1)[0]
        self.assertIn("needs: [release-info, quality, build-docker-binary]", linux)
        self.assertIn("needs: [release-info, package-assets, build-docker-image]", publish)
        mac = text.split("  publish-non-docker:\n", 1)[1].split("  publish-channel-index:\n", 1)[0]
        self.assertIn("needs: [release-info, package-non-docker-assets]", mac)
        self.assertIn("release-publication-${{ github.repository }}", mac)


if __name__ == "__main__":
    unittest.main()
