"""
Apply business transformations to silver data and create gold datasets.
Usage:
  python3 scripts/silver_to_gold_spark.py <YYYYMM>
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, year, month, substring, expr
from pathlib import Path
import sys, json

REPO_DATA_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = str(REPO_DATA_ROOT / "lake" / "silver")
GOLD_ROOT = str(REPO_DATA_ROOT / "lake" / "gold")

def main():
    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
    # read all silver parquet
    df = spark.read.parquet(f"{SILVER_ROOT}/*")

    print("Silver columns:", df.columns)

    # Ensure fecha_parsed exists or derive from ingest_month
    # derive year/month from ingest_month YYYYMM
    df = df.withColumn("year", expr("int(substr(ingest_month,1,4))")) \
           .withColumn("month", expr("int(substr(ingest_month,5,2))"))

    # Gold 1: total FOB per month
    total_fob = df.groupBy("ingest_month").agg(_sum(col("valor_fob_usd")).alias("total_fob_usd"))
    total_fob.write.mode("overwrite").parquet(f"{GOLD_ROOT}/total_fob_mes")

    # Gold 2: top companies per month
    top_comp = df.groupBy("ingest_month", "nit_exportador", "razon_social_exportador") \
                .agg(_sum(col("valor_fob_usd")).alias("total_fob")) \
                .orderBy(col("ingest_month").desc(), col("total_fob").desc())
    top_comp.write.mode("overwrite").partitionBy("ingest_month").parquet(f"{GOLD_ROOT}/top_companies_by_month")

    # Gold 3: top destinations last N months (we'll keep full aggregated table)
    top_dest = df.groupBy("ingest_month", "pais_destino_final") \
                 .agg(_sum(col("valor_fob_usd")).alias("total_fob")) \
                 .orderBy(col("ingest_month").desc(), col("total_fob").desc())
    top_dest.write.mode("overwrite").partitionBy("ingest_month").parquet(f"{GOLD_ROOT}/top_destinations_by_month")

    # Gold 4: dims - empresa, pais, producto (dedup)
    dim_emp = df.select("nit_exportador", "razon_social_exportador").dropDuplicates(["nit_exportador"])
    dim_emp.write.mode("overwrite").parquet(f"{GOLD_ROOT}/dim_empresa")

    dim_pais = df.select("pais_destino_final").dropDuplicates(["pais_destino_final"])
    dim_pais.write.mode("overwrite").parquet(f"{GOLD_ROOT}/dim_pais")

    dim_prod = df.select("subpartida").dropDuplicates(["subpartida"])
    dim_prod = dim_prod.withColumn("hs2", substring(col("subpartida"),1,2))
    dim_prod.write.mode("overwrite").parquet(f"{GOLD_ROOT}/dim_producto")

    # write metadata
    Path("metadata/etl_runs").mkdir(parents=True, exist_ok=True)
    meta = {"rows_silver": df.count()}
    with open("metadata/etl_runs/silver_to_gold_summary.json","w",encoding="utf8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    spark.stop()
    print("✅ Silver->Gold finished.")

if __name__ == "__main__":
    main()
