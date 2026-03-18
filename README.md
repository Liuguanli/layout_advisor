# Layout Exploration System Prototype

This repository is a local prototype for exploring lakehouse physical design choices from both the dataset side and the workload side. The system supports four stages:

1. dataset selection and profiling
2. workload selection and parsing
3. physical design exploration
4. verification and config preview

The codebase is split into:

- `backend/`: FastAPI service for dataset catalogs, workload catalogs, profiling, parsing, evaluation, and mock execution
- `frontend/`: Next.js UI
- `examples/`: example workload files and generation scripts

This README is written for collaborators who clone the repo onto their own machine.

## What Is Machine-Specific

The most important point is:

- `backend/app/config/dataset_catalog.py` is local-machine specific

That file currently contains absolute dataset paths. A collaborator must update those paths before the backend can load any dataset.

`backend/app/config/workload_catalog.py` is easier to share because it mostly uses project-relative paths into `examples/`.

If a collaborator can open the UI but cannot load datasets, the dataset catalog is the first place to check.

## Prerequisites

- Python 3.10+
- Node.js 18+
- `npm`

## Repository Structure

- `backend/app/config/dataset_catalog.py`: static dataset catalog, local paths
- `backend/app/config/workload_catalog.py`: static workload catalog
- `backend/app/api/routes/`: backend REST routes
- `backend/app/services/`: profiling, workload analysis, layout evaluation
- `backend/app/utils/sql_parser.py`: predicate parsing logic
- `backend/tests/`: backend unit tests
- `frontend/src/app/`: Next.js app entry and shared CSS
- `frontend/src/components/`: UI panels
- `frontend/src/lib/api.ts`: frontend API client

## Quick Start

### 1. Clone and enter the repo

```bash
git clone <your-repo-url>
cd layout_advisor
```

### 2. Configure datasets

Edit [backend/app/config/dataset_catalog.py](/Users/guanl1/Dropbox/PostDoc/topics/LakeHouse/layout_advisor/backend/app/config/dataset_catalog.py).

Each entry needs:

- `dataset_id`
- `name`
- `file_path`

Supported dataset formats:

- `.parquet`
- `.pq`
- `.csv`
- `.tbl`

`file_path` may point to:

- a single file
- a directory containing one supported data file

Example:

```python
DATASET_CATALOG = [
    {
        "dataset_id": "tpch_1_parquet",
        "name": "TPCH Dataset (tpch_1.parquet)",
        "file_path": "/absolute/path/to/tpch_1.parquet",
    },
    {
        "dataset_id": "tpch_4_parquet",
        "name": "TPCH Dataset (tpch_4.parquet)",
        "file_path": "/absolute/path/to/tpch_4.parquet",
    },
]
```

Notes:

- do not keep someone else's absolute path unless that file exists on your machine
- if your datasets are on an external drive, use the mounted absolute path visible on your machine
- if the catalog points to missing files, catalog loading or dataset selection will fail

### 3. Check workloads

Open [backend/app/config/workload_catalog.py](/Users/guanl1/Dropbox/PostDoc/topics/LakeHouse/layout_advisor/backend/app/config/workload_catalog.py).

Each workload entry needs:

- `workload_id`
- `name`
- `file_path`

The workload file format is simple:

- one SQL query per line

The repository already includes:

- `examples/queries.txt`
- `examples/queries_rich_1000.txt`

Most collaborators can keep the workload catalog unchanged unless they want to add their own workloads.

## Local Development Setup

### Backend

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload
```

Backend URLs:

- root: `http://127.0.0.1:8001`
- health: `http://127.0.0.1:8001/api/health`

Quick check:

```bash
curl http://127.0.0.1:8001/api/health
```

### Frontend

Open a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Frontend URL:

- `http://localhost:3000`

By default the frontend talks to:

- `http://127.0.0.1:8001`

If you need a different backend host or port:

```bash
cd frontend
NEXT_PUBLIC_API_BASE=http://127.0.0.1:8001 npm run dev
```

## First Run in the UI

1. Open `http://localhost:3000`
2. In `1. Dataset Selection`, choose a dataset and click `Load Dataset`
3. In `2. Query Workload Selection`, choose a workload and click `Load Workload`
4. Optionally compute correlation
5. In `3. Physical Design Exploration`, choose:
   - partition design
   - layout columns
   - layout strategy
   - permutation candidates
6. Click `Run Layout Evaluation`
7. In `4. Verification`, choose candidates and run the mock benchmark

## LAN / Same-Wi-Fi Sharing

If you want other people on the same Wi-Fi network to use your running app from your machine:

### 1. Start backend on all interfaces

```bash
cd backend
source .venv/bin/activate
python -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

### 2. Set the frontend API base to your LAN IP

From `frontend/`:

```bash
printf 'NEXT_PUBLIC_API_BASE=http://YOUR_LAN_IP:8001\n' > .env.local
```

Example:

```bash
printf 'NEXT_PUBLIC_API_BASE=http://192.168.0.238:8001\n' > .env.local
```

### 3. Start frontend on all interfaces

```bash
cd frontend
npm run dev -- --hostname 0.0.0.0 --port 3000
```

### 4. Share the LAN URL

Other people on the same Wi-Fi should open:

```text
http://YOUR_LAN_IP:3000
```

Notes:

- backend CORS is already configured for `localhost`, `127.0.0.1`, and common private LAN ranges
- only your machine needs access to the dataset files
- collaborators using your running service do not need local copies of the datasets

To find your LAN IP on macOS:

```bash
ipconfig getifaddr en0
```

## Backend API Endpoints

Main routes:

- `GET /`
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

## SQL Workload Support

The parser is aimed at `SELECT ... FROM ... WHERE ...` workloads and currently supports:

- equality: `=`
- inequality: `!=`
- range: `>`, `>=`, `<`, `<=`, `BETWEEN`
- `IN (...)`
- `LIKE 'x%'`
- `LIKE '%x'`
- `LIKE '%x%'`
- conjunction flattening over `AND`

Unsupported SQL constructs may be ignored during predicate extraction.

## Tests and Verification

### Backend tests

```bash
cd backend
source .venv/bin/activate
PYTHONPATH=. python -m unittest discover -s tests
```

### Frontend production build

```bash
cd frontend
npm run build
```

## Common Troubleshooting

### `Dataset catalog could not be loaded from the backend`

Check:

- backend is running
- backend is reachable at the configured host and port
- `dataset_catalog.py` has valid paths

### `Workload catalog could not be loaded from the backend`

Check:

- backend is running
- workload files in `workload_catalog.py` exist

### Dataset selection fails after the catalog loads

Usually this means:

- the catalog entry exists
- but `file_path` does not exist on the current machine

### Frontend opens but cannot reach the backend

Check:

- backend is running on port `8001`
- `NEXT_PUBLIC_API_BASE` matches the backend URL
- if using LAN mode, backend was started with `--host 0.0.0.0`

### Same-Wi-Fi users can open the frontend but still see backend errors

Check:

- `.env.local` points to your LAN IP, not `127.0.0.1`
- backend is bound to `0.0.0.0`
- local firewall is not blocking incoming connections

## Collaboration Notes

Before opening a PR or sharing the branch with another collaborator:

1. confirm `backend/app/config/dataset_catalog.py` is correct for your own machine
2. avoid committing personal one-off dataset paths unless the team explicitly wants them in the shared catalog
3. verify backend tests still pass
4. verify `frontend` still builds

Recommended pre-share checks:

```bash
cd backend
source .venv/bin/activate
PYTHONPATH=. python -m unittest discover -s tests
```

```bash
cd frontend
npm run build
```

## Current Limitations

- state is in-memory only
- no auth
- no persistence layer
- no job queue
- dataset and workload ingestion are catalog-based, not browser-upload-based
- correlation is computed on demand
- verification uses mock execution, not a real distributed runner
