import os
import sys
import logging
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task


LOG = logging.getLogger(__name__)

# Data folder where new files arrive
# resolved relative to this DAG file (project root assumed 3 levels up)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data_lake', 'incoming')
# scripts are under data_lake/scripts
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'data_lake', 'scripts')

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def _run_script(script_path: str, args: list[str]) -> str:
    """Run a python script with the current python executable.

    Returns the primary input path back so downstream tasks can map.
    Raises CalledProcessError on failure so Airflow marks the task failed.
    """
    cmd = [sys.executable, script_path] + args
    # run with CWD set to data_lake so scripts that use relative paths (lake/, metadata/) write
    # into the expected project folder
    script_cwd = os.path.join(PROJECT_ROOT, 'data_lake')
    LOG.info('Running: %s (cwd=%s)', ' '.join(cmd), script_cwd)
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=script_cwd)
    LOG.info('stdout:\n%s', res.stdout)
    LOG.info('stderr:\n%s', res.stderr)
    res.check_returncode()
    # If this is the ingest script, parse stdout for the yyyymm folder created
    # Example stdout contains: "Ingested file to bronze: /.../lake/bronze/202501/01_Exportaciones_2025_Enero.xlsx"
    try:
        if os.path.basename(script_path).startswith('ingest_bronze'):
            import re

            m = re.search(r"lake[/\\]bronze[/\\](\d{6})[/\\]", res.stdout)
            if m:
                return m.group(1)
            # fallback: look for metadata path printed
            m2 = re.search(r"Metadata written:\s*(.*)", res.stdout)
            if m2:
                meta = m2.group(1).strip()
                # try to parse yyyymm from metadata path
                m3 = re.search(r"/metadata[/\\]bronze[/\\](\d{6})", meta)
                if m3:
                    return m3.group(1)
    except Exception:
        LOG.exception('Failed to parse ingest output for month')

    # default: return the most-likely input path (last arg)
    return args[-1] if args else ''


@dag(
    dag_id='medallion_dynamic_taskflow',
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=1),
    catchup=False,
    default_args=default_args,
    tags=['medallion', 'etl'],
)
def medallion_elt():
    """TaskFlow-style DAG that discovers incoming files and processes each file
    through the medallion stages using dynamic task mapping.

    Contract (inputs/outputs):
    - list_files() -> list[str] full paths
    - each processing task receives a single file path and returns it on success

    Notes:
    - Requires Airflow 2.3+ for task mapping.
    - The DAG executes the existing scripts in `scripts/` using the environment's
      Python executable. Make sure the Airflow worker's env has necessary deps.
    """

    @task
    def list_files() -> list[str]:
        if not os.path.exists(DATA_DIR):
            LOG.warning('DATA_DIR does not exist: %s', DATA_DIR)
            return []
        all_files = [
            os.path.join(DATA_DIR, f)
            for f in os.listdir(DATA_DIR)
            if f.lower().endswith(('.xlsx', '.csv', '.parquet'))
        ]

        # filter out files that were already ingested (exist in lake/bronze/*/filename)
        new_files = []
        bronze_root = os.path.join(PROJECT_ROOT, 'data_lake', 'lake', 'bronze')
        for fp in all_files:
            fname = os.path.basename(fp)
            already = False
            if os.path.exists(bronze_root):
                # search for same filename under any month folder
                for root, dirs, files in os.walk(bronze_root):
                    if fname in files:
                        already = True
                        break
            if not already:
                new_files.append(fp)

        files = new_files
        files.sort()
        LOG.info('Discovered %d files', len(files))
        return files

    @task
    def ingest_bronze(file_path: str) -> str:
        script = os.path.join(SCRIPTS_DIR, 'ingest_bronze.py')
        # some ingest scripts expect --source, others positional -- try both patterns
        try:
            return _run_script(script, ['--source', file_path])
        except Exception:
            return _run_script(script, [file_path])

    @task
    def bronze_to_silver(month: str) -> str:
        """month is YYYYMM produced by ingest_bronze"""
        script = os.path.join(SCRIPTS_DIR, 'bronze_silver.py')
        return _run_script(script, [month])

    @task
    def quality_checks(month: str) -> str:
        script = os.path.join(SCRIPTS_DIR, 'quality_checks.py')
        return _run_script(script, [month])

    @task
    def silver_to_gold(month: str) -> str:
        script = os.path.join(SCRIPTS_DIR, 'silver_gold.py')
        return _run_script(script, [month])

    files = list_files()
    if files:
        ingested = ingest_bronze.expand(file_path=files)
        bronzed = bronze_to_silver.expand(month=ingested)
        checked = quality_checks.expand(month=bronzed)
        golded = silver_to_gold.expand(month=checked)
        # return final mapping so Airflow shows dependency chain
        return golded
    else:
        # No files - return an empty list (DAG run will be a no-op)
        return []


medallion_dag = medallion_elt()

