import argparse
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, min as spark_min, max as spark_max, count, collect_list
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(process)d,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Problem2_ClusterUsageAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    logger.info("Spark session created successfully for Problem 2")
    return spark

def load_log_data(spark, data_path):
    logger.info(f"Loading log data from: {data_path}")
    
    # Find all log files
    log_files = []
    for root, dirs, files in os.walk(data_path):
        for file in files:
            if file.endswith('.log'):
                log_files.append(os.path.join(root, file))
    
    logger.info(f"Found {len(log_files)} log files")
    
    if not log_files:
        raise ValueError(f"No log files found in {data_path}")
    
    logs_df = spark.read.option("input_file_name", True).text(log_files)
    logger.info(f"Loaded {logs_df.count()} log lines")
    
    return logs_df

def extract_application_info(logs_df, spark):
    logger.info("Extracting application information from log entries")
    
    schema = StructType([
        StructField("cluster_id", StringType(), True),
        StructField("application_id", StringType(), True),
        StructField("app_number", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("raw_line", StringType(), True)
    ])
    
    def extract_app_info(row):
        file_path = row['input_file']
        raw_line = row['value']
        
        app_match = re.search(r'application_(\d+)_(\d+)', file_path)
        if not app_match:
            return None
        
        cluster_id = app_match.group(1)
        app_number = app_match.group(2)
        application_id = f"application_{cluster_id}_{app_number}"
        
        timestamp_match = re.search(r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', raw_line)
        if not timestamp_match:
            return None
        
        timestamp_str = timestamp_match.group(1)
        try:
            timestamp = datetime.strptime(f"20{timestamp_str}", "%Y/%m/%d %H:%M:%S")
        except ValueError:
            return None
        
        event_type = "unknown"
        if "Starting executor" in raw_line or "Registered with driver" in raw_line:
            event_type = "start"
        elif "Driver commanded a shutdown" in raw_line or "Shutting down" in raw_line:
            event_type = "end"
        elif "ApplicationMaster" in raw_line:
            event_type = "lifecycle"
        
        return (cluster_id, application_id, app_number, timestamp, event_type, raw_line)
    
    # Use input_file_name() function to get the file path
    from pyspark.sql.functions import input_file_name
    logs_with_path = logs_df.withColumn("input_file", input_file_name())
    
    app_info_rdd = logs_with_path.rdd.map(extract_app_info).filter(lambda x: x is not None)
    app_info_df = spark.createDataFrame(app_info_rdd, schema)
    
    logger.info(f"Extracted {app_info_df.count()} application events")
    
    return app_info_df

def analyze_application_timeline(app_info_df, spark):
    logger.info("Analyzing application timeline")
    
    timeline_df = app_info_df.groupBy("cluster_id", "application_id", "app_number") \
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        ) \
        .filter(col("start_time") != col("end_time"))  # Only include apps with different start/end times
    
    logger.info(f"Found {timeline_df.count()} applications with timeline data")
    
    return timeline_df

def generate_cluster_summary(timeline_df, spark):
    logger.info("Generating cluster summary statistics")
    
    cluster_summary = timeline_df.groupBy("cluster_id") \
        .agg(
            count("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        ) \
        .orderBy(col("num_applications").desc())
    
    logger.info(f"Generated summary for {cluster_summary.count()} clusters")
    
    return cluster_summary

def generate_summary_stats(timeline_df, cluster_summary, spark):
    logger.info("Generating overall summary statistics")
    
    total_clusters = cluster_summary.count()
    total_applications = timeline_df.count()
    avg_apps_per_cluster = total_applications / total_clusters if total_clusters > 0 else 0
    
    top_clusters = cluster_summary.limit(5).collect()
    
    stats = {
        'total_clusters': total_clusters,
        'total_applications': total_applications,
        'avg_apps_per_cluster': avg_apps_per_cluster,
        'top_clusters': top_clusters
    }
    
    return stats

def save_outputs(timeline_df, cluster_summary, stats, output_dir):
    logger.info(f"Saving output files to: {output_dir}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    timeline_pandas = timeline_df.toPandas()
    cluster_summary_pandas = cluster_summary.toPandas()
    
    timeline_pandas.to_csv(f"{output_dir}/problem2_timeline.csv", index=False)
    logger.info(f"Saved timeline data to: {output_dir}/problem2_timeline.csv")
    
    cluster_summary_pandas.to_csv(f"{output_dir}/problem2_cluster_summary.csv", index=False)
    logger.info(f"Saved cluster summary to: {output_dir}/problem2_cluster_summary.csv")
    
    with open(f"{output_dir}/problem2_stats.txt", "w") as f:
        f.write(f"Total unique clusters: {stats['total_clusters']}\n")
        f.write(f"Total applications: {stats['total_applications']}\n")
        f.write(f"Average applications per cluster: {stats['avg_apps_per_cluster']:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for cluster in stats['top_clusters']:
            f.write(f"  Cluster {cluster['cluster_id']}: {cluster['num_applications']} applications\n")
    
    logger.info(f"Saved summary statistics to: {output_dir}/problem2_stats.txt")

def create_visualizations(timeline_df, cluster_summary, output_dir):
    logger.info("Creating visualizations")
    
    timeline_pandas = timeline_df.toPandas()
    cluster_summary_pandas = cluster_summary.toPandas()
    
    plt.style.use('default')
    sns.set_palette("husl")
    
    plt.figure(figsize=(12, 6))
    bar_plot = sns.barplot(data=cluster_summary_pandas, x='cluster_id', y='num_applications')
    bar_plot.set_title('Number of Applications per Cluster', fontsize=16, fontweight='bold')
    bar_plot.set_xlabel('Cluster ID', fontsize=12)
    bar_plot.set_ylabel('Number of Applications', fontsize=12)
    bar_plot.tick_params(axis='x', rotation=45)
    
    for i, v in enumerate(cluster_summary_pandas['num_applications']):
        bar_plot.text(i, v + 0.1, str(v), ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/problem2_bar_chart.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"Saved bar chart to: {output_dir}/problem2_bar_chart.png")
    
    if len(timeline_pandas) > 0:
        timeline_pandas['duration_minutes'] = (
            timeline_pandas['end_time'] - timeline_pandas['start_time']
        ).dt.total_seconds() / 60
        
        timeline_pandas = timeline_pandas[
            timeline_pandas['duration_minutes'] <= timeline_pandas['duration_minutes'].quantile(0.95)
        ]
        
        plt.figure(figsize=(15, 8))
        
        g = sns.FacetGrid(timeline_pandas, col='cluster_id', col_wrap=3, height=4)
        g.map(sns.histplot, 'duration_minutes', kde=True, alpha=0.7)
        g.set_axis_labels('Duration (minutes)', 'Count')
        g.set_titles('Cluster {col_name}')
        
        g.fig.suptitle('Application Duration Distribution by Cluster', fontsize=16, fontweight='bold')
        g.fig.subplots_adjust(top=0.9)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/problem2_density_plot.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved density plot to: {output_dir}/problem2_density_plot.png")
    else:
        logger.warning("No timeline data available for density plot")

def main():
    parser = argparse.ArgumentParser(description='Problem 2: Cluster Usage Analysis')
    parser.add_argument('--data-path', default='data/sample', 
                       help='Path to log data (default: data/sample for local testing)')
    parser.add_argument('--output-dir', default='data/output',
                       help='Output directory (default: data/output)')
    parser.add_argument('--skip-spark', action='store_true',
                       help='Skip Spark processing and regenerate visualizations from existing CSVs')
    
    args = parser.parse_args()
    
    start_time = time.time()
    logger.info("Starting Problem 2: Cluster Usage Analysis")
    
    try:
        if args.skip_spark:
            logger.info("Skipping Spark processing, regenerating visualizations from existing CSVs")
            
            timeline_df = pd.read_csv(f"{args.output_dir}/problem2_timeline.csv")
            cluster_summary_df = pd.read_csv(f"{args.output_dir}/problem2_cluster_summary.csv")
            
            create_visualizations(timeline_df, cluster_summary_df, args.output_dir)
            
        else:
            spark = create_spark_session()
            
            logs_df = load_log_data(spark, args.data_path)
            app_info_df = extract_application_info(logs_df, spark)
            timeline_df = analyze_application_timeline(app_info_df, spark)
            cluster_summary = generate_cluster_summary(timeline_df, spark)
            stats = generate_summary_stats(timeline_df, cluster_summary, spark)
            
            save_outputs(timeline_df, cluster_summary, stats, args.output_dir)
            
            create_visualizations(timeline_df, cluster_summary, args.output_dir)
            
            print("CLUSTER USAGE ANALYSIS RESULTS")
            print("=" * 50)
            print(f"Total clusters: {stats['total_clusters']}")
            print(f"Total applications: {stats['total_applications']}")
            print(f"Average applications per cluster: {stats['avg_apps_per_cluster']:.2f}")
            print("\nMost heavily used clusters:")
            for cluster in stats['top_clusters']:
                print(f"  Cluster {cluster['cluster_id']}: {cluster['num_applications']} applications")
            
            execution_time = time.time() - start_time
            logger.info("Problem 2 execution completed in %.2f seconds", execution_time)
            
            spark.stop()
            
    except Exception as e:
        logger.error("Error during execution: %s", str(e))
        raise

if __name__ == "__main__":
    main()
