"""Static workload catalog configuration.

Update this list to point to local workload files.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

WORKLOAD_CATALOG: list[dict[str, str]] = [
    {
        "workload_id": "lineitem_1000",
        "name": "TPC-H Lineitem Workload (1000 queries)",
        "file_path": str(PROJECT_ROOT / "examples" / "queries.txt"),
    },
    {
        "workload_id": "lineitem_rich_1000",
        "name": "TPC-H Lineitem Rich Workload (1000 queries)",
        "file_path": str(PROJECT_ROOT / "examples" / "queries_rich_1000.txt"),
    }
]
