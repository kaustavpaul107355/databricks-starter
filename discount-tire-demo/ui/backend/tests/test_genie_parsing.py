import sys
from pathlib import Path
import unittest

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

import server  # noqa: E402


def build_query_result(columns, values):
    return {
        "statement_response": {
            "manifest": {
                "schema": {
                    "columns": [{"name": name} for name in columns],
                    "column_count": len(columns),
                }
            },
            "result": {
                "data_typed_array": [
                    {
                        "values": [{"str": value} for value in values],
                    }
                ]
            },
        }
    }


class GenieParsingTests(unittest.TestCase):
    def test_extract_summary_prefers_text_attachment(self):
        message = {
            "attachments": [
                {"text": {"content": "The total sales for the last 7 days is 0.0."}},
                {"query": {"description": "You want to see the total sales amount."}},
            ]
        }
        summary, source = server.extract_summary(message, None, question="sales for last 7 days")
        self.assertEqual(summary, "The total sales for the last 7 days is 0.0.")
        self.assertEqual(source, "text")

    def test_extract_table_parses_result(self):
        query_result = build_query_result(["total_sales_last_7_days"], ["0.0"])
        table = server.extract_table(query_result)
        self.assertEqual(table["columns"], ["total_sales_last_7_days"])
        self.assertEqual(table["rows"], [["0.0"]])

    def test_build_summary_from_result_revenue_growth(self):
        query_result = build_query_result(
            ["quarter", "total_revenue", "total_prior_revenue", "revenue_growth"],
            ["2025-10-01T00:00:00.000Z", "76685.0", "76785.0", "-0.0013"],
        )
        summary = server.build_summary_from_result("revenue growth last quarter", query_result)
        self.assertIn("Revenue growth last quarter was", summary)
        self.assertIn("total revenue $76,685", summary)

    def test_build_summary_from_result_total_revenue(self):
        query_result = build_query_result(
            ["quarter", "total_revenue"],
            ["2025-10-01T00:00:00.000Z", "76685.0"],
        )
        summary = server.build_summary_from_result("revenue for last quarter", query_result)
        self.assertTrue(summary.startswith("The total revenue for the last quarter was $76,685"))


if __name__ == "__main__":
    unittest.main()
