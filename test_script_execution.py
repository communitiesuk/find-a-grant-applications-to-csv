import subprocess
import sys
import unittest
from pathlib import Path

class TestScriptExecution(unittest.TestCase):
    def test_script_runs_with_required_args(self):
        script = str(Path(__file__).parent / "applications_to_csv.py")
        # Use dummy values, but script should fail gracefully (not with missing arg error)
        result = subprocess.run([
            sys.executable, script,
            "--api-base", "https://example.gov.uk",
            "--ggis-reference-number", "DUMMY-REF",
            "--api-key", "DUMMY-KEY"
        ], capture_output=True, text=True)
        # Should not fail with missing argument error
        self.assertNotIn("required", result.stderr.lower())
        self.assertNotIn("missing", result.stderr.lower())
        # Should exit nonzero due to dummy API, but not due to CLI error
        self.assertNotEqual(result.returncode, 0)

if __name__ == "__main__":
    unittest.main()
