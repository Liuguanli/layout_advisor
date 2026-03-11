# Layout Exploration System Prototype

This project is a local prototype for exploring physical design choices for lakehouse-style datasets.
It lets you:

1. load a dataset from a backend catalog
2. load a SQL workload from a backend catalog
3. inspect dataset/workload statistics
4. compare partition and layout candidates
5. run a mock verification flow and preview platform-specific configuration snippets

The system is split into:

- `backend/`: FastAPI service for catalogs, profiling, workload parsing, layout evaluation, and mock execution
- `frontend/`: Next.js UI for dataset/workload analysis and physical design exploration
- `examples/`: example workload files and helper scripts

## What Is Machine-Specific

The most important setup detail is:

- `backend/app/config/dataset_catalog.py` is expected to point to dataset files on the local machine

This means the README must not assume your local dataset path matches mine. If another person clones this repo, they must update the dataset catalog before the UI can load any dataset.

`backend/app/config/workload_catalog.py` is already safer because it uses project-relative paths for the example workloads.

## Prerequisites

- Python 3.10+ recommended
- Node.js 18+ recommended
- `npm`

## Repository Structure

- `backend/app/config/dataset_catalog.py`
  machine-specific dataset entries
- `backend/app/config/workload_catalog.py`
  workload entries
- `backend/app/api/routes/`
  REST endpoints
- `backend/app/services/`
  dataset/workload/layout logic
- `backend/app/utils/sql_parser.py`
  SQL predicate parsing
- `frontend/src/components/`
  UI panels
- `frontend/src/lib/api.ts`
  frontend API client

## Step 1: Configure the Dataset Catalog

Open:

- `backend/app/config/dataset_catalog.py`

Each entry needs:

- `dataset_id`
- `name`
- `file_path`

`file_path` can be:

- a file path such as `.../tpch_1.parquet`
- a directory path containing one supported dataset file

Supported dataset formats:

- `.parquet`
- `.pq`
- `.csv`
- `.tbl`

If `file_path` is a directory, the backend will pick the first supported file it finds.

### Example

```python
DATASET_CATALOG = [
    {
        "dataset_id": "tpch_1_parquet",
        "name": "TPCH Dataset (tpch_1.parquet)",
        "file_path": "/absolute/path/to/tpch_1.parquet",
    },
    {
        "dataset_id": "orders_csv",
        "name": "Orders CSV",
        "file_path": "/absolute/path/to/orders.csv",
    },
]
```

### Important

Do not keep my personal path unless that file actually exists on your machine.

If the path is wrong, the frontend will usually show errors such as:

- `Dataset catalog could not be loaded from the backend`
- dataset loads but selection fails because the configured file does not exist

## Step 2: Configure the Workload Catalog

Open:

- `backend/app/config/workload_catalog.py`

Each workload entry needs:

- `workload_id`
- `name`
- `file_path`

The workload file should be a text file with one SQL query per line.

The current repo already includes example workload files under `examples/`, so most users can keep this file as-is unless they want their own workload.

### Example

```python
WORKLOAD_CATALOG = [
    {
        "workload_id": "lineitem_1000",
        "name": "TPC-H Lineitem Workload (1000 queries)",
        "file_path": str(PROJECT_ROOT / "examples" / "queries.txt"),
    }
]
```

## Step 3: Start the Backend

From the project root:

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8001
```

Backend URL:

- `http://127.0.0.1:8001`

Quick health check:

```bash
curl http://127.0.0.1:8001/api/health
```

## Step 4: Start the Frontend

Open a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Frontend URL:

- `http://localhost:3000`

By default the frontend uses:

- `http://127.0.0.1:8001`

for the backend API.

If you run the backend on another host or port, start the frontend like this:

```bash
cd frontend
NEXT_PUBLIC_API_BASE=http://127.0.0.1:8001 npm run dev
```

## Step 5: Use the UI

1. Open `http://localhost:3000`
2. In `Dataset Selection`, choose a dataset from the configured dataset catalog and click `Load Dataset`
3. In `Workload Selection`, choose a workload and click `Load Workload`
4. Optionally compute correlation
5. In `Physical Design Exploration`, choose:
   - partition strategy
   - partition columns
   - layout columns
   - layout strategy
   - permutation candidates
6. Click `Run Layout Evaluation`
7. In `Verification`, choose candidates and run the mock benchmark

## Endpoints

Main backend routes:

- `GET /api/health`
- `GET /api/dataset/catalog`
- `POST /api/dataset/select`
- `GET /api/dataset/summary`
- `GET /api/dataset/correlation`
- `POST /api/dataset/profile-sample`
- `GET /api/workload/catalog`
- `POST /api/workload/select`
- `GET /api/workload/summary`
- `POST /api/layout/evaluate`
- `POST /api/layout/mock-execute`

## Supported Workload Parsing

The parser currently focuses on `SELECT ... FROM ... WHERE ...` style workloads and supports common predicate patterns such as:

- equality: `=`
- inequality: `!=`
- range: `>`, `>=`, `<`, `<=`, `BETWEEN`
- `IN (...)`
- prefix `LIKE 'x%'`
- suffix `LIKE '%x'`
- contains `LIKE '%x%'`
- conjunction flattening with `AND`

Unsupported SQL patterns may be ignored during predicate extraction.

## Troubleshooting

### Dataset catalog cannot be loaded

Check:

- backend is running on `127.0.0.1:8001`
- `backend/app/config/dataset_catalog.py` has valid local paths
- the configured dataset files actually exist

### Workload catalog cannot be loaded

Check:

- backend is running
- `backend/app/config/workload_catalog.py` points to real workload files

### Frontend cannot reach the backend

Check:

- backend port is `8001`
- `NEXT_PUBLIC_API_BASE` matches the backend URL

### Dataset selection fails after catalog loads

Usually this means:

- the catalog entry exists
- but `file_path` is invalid on the current machine

### Python environment issues

Use the backend virtual environment explicitly:

```bash
cd backend
source .venv/bin/activate
which python
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8001
```

## Optional Backend Test

```bash
cd backend
source .venv/bin/activate
PYTHONPATH=. python -m unittest discover -s tests
```

## Notes

- State is in-memory only
- There is no persistence, auth, or job queue
- Dataset/workload ingestion in this prototype is catalog-based, not browser upload
- Correlation is computed on demand
- Layout evaluation is partly real and partly mock:
  - `no layout` and `linear` use the sample-based evaluation path
  - `zorder` and `hilbert` still rely on deterministic mock behavior

## Recommended First-Time Setup Checklist

Before telling someone else to run the pipeline, verify these four things:

1. `backend/app/config/dataset_catalog.py` points to real files on their machine
2. `backend/app/config/workload_catalog.py` points to real workload files
3. backend starts on port `8001`
4. frontend can load both catalogs from the UI
