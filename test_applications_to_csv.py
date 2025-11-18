import unittest
from find_a_grant_csv.csv_utils import extract_row, sanitize_col


class TestApplicationsToCsv(unittest.TestCase):
    def setUp(self) -> None:
        self.root_meta = {
            "applicationFormName": "Test Form",
            "applicationFormVersion": "1.0",
            "applicationId": "A1",
            "ggisReferenceNumber": "GGIS-1234",
            "grantAdminEmailAddress": "admin@example.com",
        }
        self.sub = {
            "submissionId": "S1",
            "grantApplicantEmailAddress": "user@example.com",
            "submittedTimeStamp": "2025-11-18T12:00:00Z",
            "gapId": "GAP-1",
            "sections": [
                {
                    "sectionTitle": "Section 1",
                    "questions": [
                        {
                            "questionTitle": "Q1",
                            "questionId": "Q1",
                            "questionResponse": "Answer 1",
                        },
                        {
                            "questionTitle": "Q2",
                            "questionId": "Q2",
                            "questionResponse": 42,
                        },
                    ],
                },
                {
                    "sectionTitle": "Section 2",
                    "questions": [
                        {
                            "questionTitle": "Q3",
                            "questionId": "Q3",
                            "questionResponse": [1, 2, 3],
                        },
                        {
                            "questionTitle": "Q4",
                            "questionId": "Q4",
                            "questionResponse": {"a": 1, "b": 2},
                        },
                    ],
                },
            ],
        }

    def test_extract_row_structure(self) -> None:
        meta, dyn, blocks = extract_row(
            self.root_meta,
            self.sub,
            include_qid=False,
            prefix_section=False,
            add_section_separators=True,
        )
        # Check meta fields
        self.assertIn(sanitize_col("applicationFormName"), meta)
        self.assertIn(sanitize_col("submissionId"), meta)
        # Check dynamic fields
        self.assertIn("Section: Section 1", dyn)
        self.assertIn("Q1", dyn)
        self.assertIn("Q2", dyn)
        self.assertIn("Section: Section 2", dyn)
        self.assertIn("Q3", dyn)
        self.assertIn("Q4_a", dyn)
        self.assertIn("Q4_b", dyn)
        # Check blocks structure
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0][0], "Section: Section 1")
        self.assertIn("Q1", blocks[0][1])
        self.assertIn("Q2", blocks[0][1])

    def test_default_filename_snake_case(self) -> None:
        # Simulate the filename logic for applicationFormName
        def to_snake_case(s: str) -> str:
            import re

            s = re.sub(r"[^A-Za-z0-9]+", "_", s)
            s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
            return s.strip("_").lower()

        app_name = self.root_meta["applicationFormName"]
        today = __import__("datetime").date.today()
        expected = f"{to_snake_case(app_name)}-{today.year}-{today.month:02d}-{today.day:02d}.csv"
        self.assertEqual(
            expected, f"test_form-{today.year}-{today.month:02d}-{today.day:02d}.csv"
        )


if __name__ == "__main__":
    unittest.main()
