from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("GCS to BigQuery").getOrCreate()

# Set the GCS bucket path for the CSV file
gcs_path = "gs://walmart_dataproc/Walmart.csv"

# Read the CSV file from GCS
df = spark.read.format("csv").option("header", "true").load(gcs_path)

# Set the BigQuery output project ID, dataset, and table name
output_project_id = "priyankachoudhary-1679982189"
output_dataset = "dataproctest"
output_table = "dataproctable"

# Write the DataFrame to BigQuery
df.write.format("bigquery") \
    .option("temporaryGcsBucket", "gs://walmart_dataproc") \
    .option("project", output_project_id) \
    .option("dataset", output_dataset) \
    .option("table", output_table) \
    .mode("overwrite") \
    .save()

# Stop the SparkSession
spark.stop()
