"""Static dataset catalog configuration.

Update this list to point to your local datasets.
"""

DATASET_CATALOG: list[dict[str, str]] = [
    {
        "dataset_id": "tpch_1_parquet",
        "name": "TPCH Dataset (tpch_1.parquet)",
        "file_path": "/Users/guanlil1/Downloads/tpch_1.parquet",
    },
    {
        "dataset_id": "tpch_4_parquet",
        "name": "TPCH Dataset (tpch_4.parquet)",
        "file_path": "/Users/guanlil1/Downloads/tpch_4.parquet",
    },
]

# DATASET_CATALOG: list[dict[str, str]] = [
#     {
#         "dataset_id": "tpch_1_parquet",
#         "name": "TPCH Dataset (tpch_1.parquet)",
#         "file_path": "/Volumes/T7/Ubuntu_Datasets/tpch/tpch_1.parquet",
#     },
#     {
#         "dataset_id": "tpch_4_parquet",
#         "name": "TPCH Dataset (tpch_4.parquet)",
#         "file_path": "/Volumes/T7/Ubuntu_Datasets/tpch/tpch_4.parquet",
#     },
#     {
#         "dataset_id": "tpch_16_parquet",
#         "name": "TPCH Dataset (tpch_16.parquet)",
#         "file_path": "/Volumes/T7/Ubuntu_Datasets/tpch/tpch_16.parquet",
#     },
# ]
