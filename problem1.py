import os
import sys
import time
import logging
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, count, lit, when, isnan, isnull,
    desc, asc, rand, row_number
)
from pyspark.sql.types import StringType
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session():  
    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        
        .config("spark.master", "local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        .config("spark.eventLog.enabled", "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully for Problem 1")
    return spark


def load_log_data(spark, data_path):
    logger.info("Loading log data from: %s", data_path)
    import glob
    log_files = glob.glob(f"{data_path}/application_*/*.log")
    logger.info("Found %d log files", len(log_files))
    
    if not log_files:
        raise ValueError(f"No log files found in {data_path}")
    logs_df = spark.read.text(log_files)
    
    logger.info("Loaded %d log lines", logs_df.count())
    return logs_df


def parse_log_entries(logs_df):
    logger.info("Parsing log entries to extract log levels")
    parsed_logs = logs_df.select(
        col("value").alias("raw_line"),
        
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp"),
        
        when(
            regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(INFO|WARN|ERROR|DEBUG)", 2) != "",
            regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(INFO|WARN|ERROR|DEBUG)", 2)
        ).when(
            regexp_extract(col("value"), r"log4j:(ERROR|WARN|INFO|DEBUG)", 1) != "",
            regexp_extract(col("value"), r"log4j:(ERROR|WARN|INFO|DEBUG)", 1)
        ).when(
            regexp_extract(col("value"), r"\b(INFO|WARN|ERROR|DEBUG)\b", 1) != "",
            regexp_extract(col("value"), r"\b(INFO|WARN|ERROR|DEBUG)\b", 1)
        ).otherwise("UNKNOWN").alias("log_level"),
        
        when(
            regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(INFO|WARN|ERROR|DEBUG)\s+([^:]+):", 3) != "",
            regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(INFO|WARN|ERROR|DEBUG)\s+([^:]+):", 3)
        ).when(
            regexp_extract(col("value"), r"log4j:(ERROR|WARN|INFO|DEBUG)", 1) != "",
            lit("log4j")
        ).otherwise("UNKNOWN").alias("component")
    )
    
    valid_logs = parsed_logs.filter(col("log_level") != "UNKNOWN")
    
    logger.info("Parsed %d valid log entries", valid_logs.count())
    return valid_logs


def analyze_log_levels(parsed_logs, spark_session):
    logger.info("Analyzing log level distribution")
    
    log_level_counts = (
        parsed_logs
        .groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
    )
    
    expected_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    existing_levels = [row["log_level"] for row in log_level_counts.collect()]
    
    missing_levels = [level for level in expected_levels if level not in existing_levels]
    if missing_levels:
        from pyspark.sql import Row
        missing_rows = [Row(log_level=level, count=0) for level in missing_levels]
        missing_df = spark_session.createDataFrame(missing_rows)
        log_level_counts = log_level_counts.union(missing_df)
    
    log_level_counts = log_level_counts.orderBy(
        when(col("log_level") == "INFO", 1)
        .when(col("log_level") == "WARN", 2)
        .when(col("log_level") == "ERROR", 3)
        .when(col("log_level") == "DEBUG", 4)
        .otherwise(5)
    )
    
    return log_level_counts


def get_sample_entries(parsed_logs, sample_size=10):
    logger.info("Selecting %d random sample log entries", sample_size)
    sample_entries = (
        parsed_logs
        .withColumn("rand", rand())
        .orderBy("rand")
        .limit(sample_size)
        .select("log_level", "component", "timestamp", "raw_line")
    )
    
    return sample_entries


def generate_summary_stats(parsed_logs, log_level_counts):
    logger.info("Generating summary statistics")
    
    total_entries = parsed_logs.count()
    
    counts_pd = log_level_counts.toPandas()
    
    counts_pd['percentage'] = (counts_pd['count'] / total_entries * 100).round(2)
    
    component_counts = (
        parsed_logs
        .groupBy("component")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
        .limit(1)
        .collect()
    )
    
    most_common_component = component_counts[0]["component"] if component_counts else "UNKNOWN"
    most_common_count = component_counts[0]["count"] if component_counts else 0
    
    date_range = (
        parsed_logs
        .filter(col("timestamp") != "")
        .select("timestamp")
        .distinct()
        .orderBy("timestamp")
        .collect()
    )
    
    start_date = date_range[0]["timestamp"] if date_range else "UNKNOWN"
    end_date = date_range[-1]["timestamp"] if date_range else "UNKNOWN"
    
    return {
        "total_entries": total_entries,
        "log_level_distribution": counts_pd,
        "most_common_component": most_common_component,
        "most_common_component_count": most_common_count,
        "date_range": (start_date, end_date)
    }


def save_outputs(log_level_counts, sample_entries, summary_stats, output_dir):
    logger.info("Saving output files to: %s", output_dir)
    
    os.makedirs(output_dir, exist_ok=True)
    
    counts_path = os.path.join(output_dir, "problem1_counts.csv")
    log_level_counts.toPandas().to_csv(counts_path, index=False)
    logger.info("Saved log level counts to: %s", counts_path)
    
    sample_path = os.path.join(output_dir, "problem1_sample.csv")
    sample_df = sample_entries.select(
        col("raw_line").alias("log_entry"),
        col("log_level")
    ).toPandas()
    sample_df.to_csv(sample_path, index=False)
    logger.info("Saved sample entries to: %s", sample_path)
    
    summary_path = os.path.join(output_dir, "problem1_summary.txt")
    with open(summary_path, 'w') as f:
        f.write(f"Total log lines processed: {summary_stats['total_entries']:,}\n")
        f.write(f"Total lines with log levels: {summary_stats['total_entries']:,}\n")
        f.write(f"Unique log levels found: {len(summary_stats['log_level_distribution'])}\n\n")
        
        f.write("Log level distribution:\n")
        for _, row in summary_stats['log_level_distribution'].iterrows():
            f.write(f"  {row['log_level']:4}  : {row['count']:8,} ({row['percentage']:5.2f}%)\n")
    
    logger.info("Saved summary statistics to: %s", summary_path)


def main():
    start_time = time.time()
    logger.info("Starting Problem 1: Log Level Distribution Analysis")
    
    try:
        spark = create_spark_session()
        
        data_path = "data/raw"
        
        logs_df = load_log_data(spark, data_path)
        parsed_logs = parse_log_entries(logs_df)
        log_level_counts = analyze_log_levels(parsed_logs, spark)
        sample_entries = get_sample_entries(parsed_logs, sample_size=10)
        summary_stats = generate_summary_stats(parsed_logs, log_level_counts)
        save_outputs(log_level_counts, sample_entries, summary_stats, "data/output")
        print("LOG LEVEL DISTRIBUTION RESULTS")
        log_level_counts.show()
        print("\n" + "="*60)
        print("SAMPLE LOG ENTRIES")
        sample_entries.show(truncate=False)
        
        print(f"\nTotal entries processed: {summary_stats['total_entries']:,}")
        print(f"Most common component: {summary_stats['most_common_component']}")
        print(f"Date range: {summary_stats['date_range'][0]} to {summary_stats['date_range'][1]}")
        
        execution_time = time.time() - start_time
        logger.info("Problem 1 execution completed in %.2f seconds", execution_time)
        
    except Exception as e:
        logger.error("Error during execution: %s", str(e))
        raise
    finally:
        if 'spark' in locals():
            logger.info("Stopping Spark session")
            spark.stop()


if __name__ == "__main__":
    main()
