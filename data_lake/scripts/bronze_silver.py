"""
Transform Bronze layer Excel files to Silver layer Parquet using PySpark.
Usage:
  python3 scripts/bronze_to_silver_spark.py <YYYYMM>
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws, to_date, regexp_replace, when, to_date, current_timestamp
from pyspark.sql.types import DoubleType
from pathlib import Path
import sys
import json
import pandas as pd

REPO_DATA_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = str(REPO_DATA_ROOT / "lake" / "bronze")
SILVER_ROOT = str(REPO_DATA_ROOT / "lake" / "silver")
META_ROOT = str(REPO_DATA_ROOT / "metadata" / "etl_runs")


def convert_excel_to_csv(raw_dir: str):
    """Convert all .xlsx files in raw_dir to .csv if not already done."""
    excel_files = list(Path(raw_dir).glob("*.xlsx"))
    for f in excel_files:
        csv_path = f.with_suffix(".csv")
        if not csv_path.exists():
            print(f"Converting {f.name} -> {csv_path.name}")
            df = pd.read_excel(f)
            df.to_csv(csv_path, index=False, sep="|", encoding="utf-8")
    if not excel_files:
        print("No Excel files found — assuming CSV already exists.")


def main(month):
    raw_dir = f"{RAW_ROOT}/{month}"
    convert_excel_to_csv(raw_dir)

    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

    df = spark.read.option("header", True).option("sep", "|").option("encoding", "utf-8").csv(f"{raw_dir}/*.csv")
    print(df.columns)


    # normalize column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())

    # clean numeric and date fields
    df = df.withColumn("valor_fob_usd_clean", regexp_replace(col("valor_fob_usd"), ",", ""))
    df = df.withColumn("valor_fob_usd_num", col("valor_fob_usd_clean").cast(DoubleType()))

    df = df.withColumn("valor_fob_pesos_clean", regexp_replace(col("valor_fob_pesos"), ",", ""))
    df = df.withColumn("valor_fob_pesos_num", col("valor_fob_pesos_clean").cast(DoubleType()))

    df = df.withColumn("cantidad_unidades_fisicas_num", regexp_replace(col("cantidad_unidades_fisicas"), ",", "").cast(DoubleType()))

    df = df.withColumn(
        "fecha_declaracion_exportacion_parsed",
        when(col("fecha_declaracion_exportacion").rlike("^[0-9]{8}$"),
            to_date(col("fecha_declaracion_exportacion"), "yyyyMMdd"))
        .otherwise(to_date(col("fecha_declaracion_exportacion"), "yyyy-MM-dd"))
    )

    # audit columns
    df = df.withColumn("ingest_month", lit(month))
    df = df.withColumn("ingest_ts", current_timestamp())
    df = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256))

    # drop intermediates (optional)
    df = df.drop("valor_fob_usd_clean", "valor_fob_pesos_clean")

    out_path = f"{SILVER_ROOT}/{month}"
    (
        df
        .repartition(1)  # <- forces a single file per partition
        .write
        .mode("overwrite")
        .parquet(out_path)
    )

    # write metadata
    Path(META_ROOT).mkdir(parents=True, exist_ok=True)
    meta = {"month": month, "rows_written": df.count(), "path": out_path}
    with open(f"{META_ROOT}/bronze_to_silver_{month}.json", "w", encoding="utf8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    spark.stop()
    print(f"✅ Bronze→Silver finished for {month}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 bronze_to_silver_spark.py <YYYYMM>")
        sys.exit(1)
    month = sys.argv[1]
    main(month)
