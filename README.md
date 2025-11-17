# Data Mendez LTDA — Medallion Data Lake

This repository contains a simple medallion architecture (Bronze → Silver → Gold) implemented for a data engineering project using local files and Airflow for orchestration. It also includes a notebook that answers client questions and two additional analyses.

Project structure

- data_lake/
  - incoming/           # raw files dropped here to be ingested
  - lake/
    - bronze/           # raw/landing organized by ingest month
    - silver/           # cleaned and normalized parquet datasets
    - gold/             # aggregated datasets used for reporting/BI
  - metadata/           # small metadata files (processed files, checksums, quality reports)
  - orchestration/
    - dags/             # Airflow DAG(s) used to orchestrate the pipeline
  - scripts/            # ETL scripts executed by the DAG (ingest_bronze, bronze_silver, silver_gold, quality_checks)
  - notebooks/
    - analysis.ipynb    # Notebook answering client questions + 2 extra analyses (Pareto + product trends)

What this repo does

- Watches `data_lake/incoming/` for new files.
- Ingests new files into a Bronze partitioned layout by YYYYMM.
- Transforms Bronze → Silver (cleaning, normalization).
- Aggregates Silver → Gold (report-ready tables like `total_fob_mes`, `top_companies_by_month`, `top_destinations_by_month`, `dim_producto`).
- Runs quality checks and writes metadata.
- Provides a notebook (`data_lake/notebooks/analysis.ipynb`) that reads Gold/Silver and produces analyses and plots.

Quick start (local)

1. Create a Python virtual environment and install dependencies. This project expects Python 3.8+.

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r data_lake/requirements.txt
   ```

2. Install Airflow (if you plan to run the DAG). We recommend installing Airflow in the same virtualenv.

   Follow the official Apache Airflow install instructions for your OS and desired executor. A minimal example:

   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow_home
   pip install 'apache-airflow==2.7.1' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
   airflow db init
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
   airflow webserver --port 8080 &
   airflow scheduler &
   ```

   Then copy the DAG file `data_lake/orchestration/dags/elt.py` into your Airflow dags folder or configure `airflow.cfg` to include `data_lake/orchestration/dags`.

3. Run the ETL manually (optional, useful for debugging).

   - Ingest a file to Bronze:
     ```bash
     python data_lake/scripts/ingest_bronze.py path/to/your/file.csv
     ```

   - Run Bronze → Silver:
     ```bash
     python data_lake/scripts/bronze_silver.py YYYYMM
     ```

   - Run Silver → Gold:
     ```bash
     python data_lake/scripts/silver_gold.py YYYYMM
     ```

   - Run quality checks:
     ```bash
     python data_lake/scripts/quality_checks.py YYYYMM
     ```

4. Open the Notebook

   - Launch Jupyter Lab / Notebook in the repo root (with the same virtualenv active):

   ```bash
   jupyter lab
   ```

   - Open `data_lake/notebooks/analysis.ipynb` and run the cells in order. The notebook uses a robust parquet reader (Spark / pyarrow / pandas fallbacks) and includes data-cleaning logic for messy numeric strings.

Notes & troubleshooting

- Paths: the scripts compute `REPO_DATA_ROOT` relative to their location to ensure they write under `data_lake/`. Run scripts from the repo root or let Airflow run them with `cwd` set to the project root.

- Dependencies: ensure the notebook kernel and Airflow workers share compatible packages (notably `pyarrow`, `pandas`, `fastparquet`, and optionally `pyspark` if you want Spark-backed reads). Mismatched environments can cause read failures or schema inference errors.

- Airflow and virtualenv: Airflow must be installed and run from the same Python environment you use for the scripts and dependencies.

- Idempotency: The DAG currently avoids reprocessing files by checking for existing bronze partitions by YYYYMM. For stronger deduplication, consider computing and storing file checksums in `data_lake/metadata/processed_files.json` and comparing checksums before ingesting.

- Debugging logs: if a DAG task fails, check Airflow task logs (they include subprocess stdout/stderr). The ingest script prints the created YYYYMM which is used by the DAG for mapping downstream tasks.

Next improvements (suggested)

- Replace subprocess calls in the DAG with direct function imports for better testability.
- Implement checksum-based processed-file index for robust deduplication.
- Add unit tests and a lightweight smoke test that runs the ETL for a tiny sample file.

Author

- Author: Mariana Gonzalez G
