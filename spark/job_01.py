"""Cobertura por indicador y década: RDD, DataFrame y Spark SQL."""

import sqlite3
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[1]
INDICATORS = ROOT / "data" / "clean" / "indicators"
SQLITE_DB = ROOT / "data" / "serving" / "observatorio.sqlite"
OUTPUT = ROOT / "data" / "serving" / "spark" / "cobertura_decada"
EVENT_LOG = ROOT / "logs" / "spark-events"


def main() -> int:
    EVENT_LOG.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder
        .appName("Cobertura por indicador y década")
        .master("local[*]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file:///{EVENT_LOG.as_posix().lstrip('/')}")
        .getOrCreate()
    )
    try:
        df = spark.read.parquet(str(INDICATORS))
        print("Number of partitions:", df.rdd.getNumPartitions())
        df.explain("formatted")

        df = df.filter(F.col("value").isNotNull())
        df = df.withColumn("decade", (F.col("year") // 10) * 10)

        rdd_counts = {
            key: count
            for key, count in (
                df.rdd
                .map(lambda row: ((row["indicator_id"], int(row["decade"])), 1))
                .reduceByKey(lambda left, right: left + right)
                .collect()
            )
        }

        dataframe_result = df.groupBy("indicator_id", "decade").agg(
            F.count("*").alias("row_count"),
            F.countDistinct("iso3").alias("country_count"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        dataframe_counts = {
            (row["indicator_id"], int(row["decade"])): int(row["row_count"])
            for row in dataframe_result.collect()
        }

        df.createOrReplaceTempView("indicators")
        sql_result = spark.sql(
            """
            SELECT indicator_id,
                   (year // 10) * 10 AS decade,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT iso3) AS country_count,
                   MIN(year) AS first_year,
                   MAX(year) AS last_year
            FROM indicators
            WHERE value IS NOT NULL
            GROUP BY indicator_id, (year // 10) * 10
            """
        )
        sql_counts = {
            (row["indicator_id"], int(row["decade"])): int(row["row_count"])
            for row in sql_result.collect()
        }

        sqlite_counts = {}
        if SQLITE_DB.exists():
            with sqlite3.connect(SQLITE_DB) as conn:
                sqlite_counts = {
                    (indicator_id, int(decade)): int(count)
                    for indicator_id, decade, count in conn.execute(
                        """
                        SELECT indicator_id,
                               (CAST(year AS INTEGER) / 10) * 10 AS decade,
                               COUNT(*) AS row_count
                        FROM indicators
                        WHERE value IS NOT NULL
                        GROUP BY indicator_id, (CAST(year AS INTEGER) / 10) * 10
                        """
                    ).fetchall()
                }

        if rdd_counts != dataframe_counts or dataframe_counts != sql_counts:
            raise SystemExit("RDD, DataFrame and Spark SQL coverage counts differ")
        if sqlite_counts and dataframe_counts != sqlite_counts:
            raise SystemExit("DataFrame and SQLite coverage counts differ")

        dataframe_result.write.mode("overwrite").parquet(str(OUTPUT))
        print(f"Coverage written to {OUTPUT}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
