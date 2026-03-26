from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def save_table(df, table_name):
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(table_name)

def silver_sales_transform(spark):
    df = spark.table("iphone_analytics.bronze_sales")

    clean_df = (
        df.withColumn("sale_id", col("sale_id").cast("int"))
          .withColumn("product_id", col("product_id").cast("int"))
          .withColumn("customer_id", col("customer_id").cast("int"))
          .withColumn("store_id", col("store_id").cast("int"))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("sale_date", to_date(col("sale_date")))
    )

    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("sale_date")
        .format("parquet")
        .saveAsTable("silver_sales")
    )

    return "silver_sales"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("silver_layer") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS iphone_analytics")
    spark.sql("USE iphone_analytics")

    bronze_customers = spark.table("iphone_analytics.bronze_customers")
    silver_customers = bronze_customers.withColumn("customer_id", col("customer_id").cast("int"))
    save_table(silver_customers, "silver_customers")

    bronze_products = spark.table("iphone_analytics.bronze_products")
    silver_products = bronze_products \
        .withColumn("product_id", col("product_id").cast("int")) \
        .withColumn("unit_price", col("unit_price").cast("int"))
    save_table(silver_products, "silver_products")

    bronze_stores = spark.table("iphone_analytics.bronze_stores")
    silver_stores = bronze_stores.withColumn("store_id", col("store_id").cast("int"))
    save_table(silver_stores, "silver_stores")

    silver_sales_transform(spark)

    spark.sql("SHOW TABLES").show(truncate=False)

    spark.stop()