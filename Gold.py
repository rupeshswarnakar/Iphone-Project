from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

def save_dimension(df, table_name):
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(table_name)

def build_dim_date(spark):
    sales = spark.table("iphone_analytics.silver_sales")

    dim_date = sales.select("sale_date") \
        .distinct() \
        .withColumn("year", year(col("sale_date"))) \
        .withColumn("month", month(col("sale_date"))) \
        .withColumn("day", dayofmonth(col("sale_date"))) \
        .withColumnRenamed("sale_date", "date_key")

    save_dimension(dim_date, "dim_date")
    return "dim_date"

def load_fact_sales(spark):
    sales = spark.table("iphone_analytics.silver_sales")
    products = spark.table("iphone_analytics.silver_products")

    fact_df = (
        sales.join(products, "product_id")
             .withColumn("total_amount", col("quantity") * col("unit_price"))
             .select(
                 "sale_id",
                 "customer_id",
                 "product_id",
                 "store_id",
                 col("sale_date").alias("date_key"),
                 "quantity",
                 "total_amount"
             )
    )

    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("date_key")
        .format("parquet")
        .saveAsTable("fact_sales")
    )

    return "fact_sales"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("gold_layer") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS iphone_analytics")
    spark.sql("USE iphone_analytics")

    dim_customer = spark.table("iphone_analytics.silver_customers")
    save_dimension(dim_customer, "dim_customer")

    dim_product = spark.table("iphone_analytics.silver_products")
    save_dimension(dim_product, "dim_product")

    dim_store = spark.table("iphone_analytics.silver_stores")
    save_dimension(dim_store, "dim_store")

    build_dim_date(spark)
    load_fact_sales(spark)

    spark.sql("SHOW TABLES").show(truncate=False)

    spark.stop()