import os
import subprocess
import sys
import unittest

class TestDockerhubCleanupE2E(unittest.TestCase):
    def _run(self, extra_args):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        script_path = os.path.join(base_dir, "dockerhub_cleanup.py")
        input_json_path = os.path.join(os.path.dirname(__file__), "dockerhub_backup_mock.json")
        cmd = [
            sys.executable,
            script_path,
            "--namespace", "testnamespace",
            "--dry-run",
            "--input-json", input_json_path,
        ] + extra_args
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, msg=f"Exit code: {result.returncode}; stderr: {result.stderr}")
        return result.stdout

    def test_prefix_preserve_rules(self):
        # preserve-last 0 so only the explicit prefix rules protect tags
        stdout = self._run(["--preserve-last", "0", "--preserve", "pr-001:1", "pr-002:1"])
        self.assertIn("Would delete", stdout)
        self.assertNotIn("pr-001", stdout)
        self.assertNotIn("pr-002", stdout)

    def test_preserve_last_is_unconditional_even_when_preserve_rules_set(self):
        # Regression: preserve_last must protect the newest N tags regardless of
        # whether --preserve rules are also specified and regardless of tag age.
        # All mock tags are from 2023, well outside any retention window.
        # test-repo-1 has two tags: pr-002 (newer) and pr-001 (older).
        # With preserve_last=1 and preserve=latest (no tag is named "latest"),
        # pr-002 must be kept and pr-001 must be deleted.
        stdout = self._run([
            "--repos", "test-repo-1",
            "--preserve-last", "1",
            "--preserve", "latest",
            "--retention-days", "30",
        ])
        self.assertNotIn("pr-002", stdout)   # newest — must be preserved
        self.assertIn("pr-001", stdout)       # older — must be deleted

if __name__ == "__main__":
    unittest.main()
