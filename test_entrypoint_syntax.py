import unittest
import subprocess
import sys
from pathlib import Path


class TestEntrypointSyntax(unittest.TestCase):
    def test_applications_to_csv_syntax(self) -> None:
        script = Path(__file__).parent / "applications_to_csv.py"
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(script)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"Syntax error in applications_to_csv.py: {result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
