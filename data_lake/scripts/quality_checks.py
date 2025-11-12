from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json, sys
from pathlib import Path

# ensure paths are rooted at data_lake
REPO_DATA_ROOT = Path(__file__).resolve().parents[1]
METADATA_ROOT = REPO_DATA_ROOT / "metadata" / "etl_runs"
SILVER_ROOT = REPO_DATA_ROOT / "lake" / "silver"

def run_checks(month):
    spark = SparkSession.builder.appName("quality_checks").getOrCreate()
    df = spark.read.parquet(str(SILVER_ROOT / month))
    results = {}

    # Basic counts
    results['rows'] = df.count()

    # Use numeric column for validations
    results['null_valorfob'] = df.filter(col("valor_fob_usd_num").isNull()).count()
    results['negative_valorfob'] = df.filter(col("valor_fob_usd_num") < 0).count()

    # NIT check
    results['null_nit_exportador'] = df.filter(col("nit_exportador").isNull() | (col("nit_exportador") == "")).count()

    # Optional uniqueness check if available
    if "numero_serie" in df.columns:
        total = df.count()
        uniq = df.select("numero_serie").dropDuplicates().count()
        results['numero_serie_unique_ratio'] = uniq / total if total > 0 else None

    # Write output
    out = METADATA_ROOT / f"quality_{month}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    spark.stop()
    print("✅ Quality checks written to", out)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/quality_checks.py <YYYYMM>")
        sys.exit(1)
    run_checks(sys.argv[1])
