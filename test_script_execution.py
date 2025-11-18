import subprocess
import sys
import unittest
from pathlib import Path


class TestScriptExecution(unittest.TestCase):
    def test_script_runs_with_required_args(self) -> None:
        script = str(Path(__file__).parent / "applications_to_csv.py")
        # Use dummy values, but script should fail gracefully (not with missing arg error)
        result = subprocess.run(
            [
                sys.executable,
                script,
                "--api-base",
                "https://example.gov.uk",
                "--ggis-reference-number",
                "DUMMY-REF",
                "--api-key",
                "DUMMY-KEY",
            ],
            capture_output=True,
            text=True,
        )
        # Should not fail with missing argument error
        self.assertNotIn("required", result.stderr.lower())
        self.assertNotIn("missing", result.stderr.lower())
        # Should not fail with AttributeError or similar Python errors
        self.assertNotIn("attributeerror", result.stderr.lower())
        self.assertNotIn("typeerror", result.stderr.lower())
        self.assertNotIn("keyerror", result.stderr.lower())
        self.assertNotIn("nameerror", result.stderr.lower())
        # Should exit nonzero due to dummy API, but not due to CLI error
        self.assertNotEqual(result.returncode, 0)

    def test_script_fails_gracefully_on_bad_api(self) -> None:
        script = str(Path(__file__).parent / "applications_to_csv.py")
        # Use obviously bad API URL
        result = subprocess.run(
            [
                sys.executable,
                script,
                "--api-base",
                "https://bad.example.gov.uk",
                "--ggis-reference-number",
                "DUMMY-REF",
                "--api-key",
                "DUMMY-KEY",
            ],
            capture_output=True,
            text=True,
        )
        # Should not fail with Python errors
        self.assertNotIn("attributeerror", result.stderr.lower())
        self.assertNotIn("typeerror", result.stderr.lower())
        self.assertNotIn("keyerror", result.stderr.lower())
        self.assertNotIn("nameerror", result.stderr.lower())
        # Should exit nonzero due to API error
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
