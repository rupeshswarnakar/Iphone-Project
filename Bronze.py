from pyspark.sql import SparkSession

def bronze_ingestion(spark, csv_path, table_name):
    df = spark.read.option("header", "true").csv(csv_path)

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"bronze_{table_name}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("bronze_layer") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS iphone_analytics")
    spark.sql("USE iphone_analytics")

    bronze_ingestion(spark, "/iphone_project/raw/customers.csv", "customers")
    bronze_ingestion(spark, "/iphone_project/raw/products.csv", "products")
    bronze_ingestion(spark, "/iphone_project/raw/sales.csv", "sales")
    bronze_ingestion(spark, "/iphone_project/raw/stores.csv", "stores")

    spark.sql("SHOW TABLES").show(truncate=False)

    spark.stop()