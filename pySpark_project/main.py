from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_extract

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Data Processing") \
    .getOrCreate()

# replace your File paths
file_region_a = "order_region_a.csv"
file_region_b = "order_region_b.csv"

# Read data from both files
region_a = spark.read.csv(file_region_a, header=True, inferSchema=True)
region_b = spark.read.csv(file_region_b, header=True, inferSchema=True)

# Add region column
region_a = region_a.withColumn("region", expr("'A'"))
region_b = region_b.withColumn("region", expr("'B'"))

# Combine data 
sales_data = region_a.union(region_b)

# Extract the numeric value from PromotionDiscount
sales_data = sales_data.withColumn("PromotionDiscountAmount",
    regexp_extract(col("PromotionDiscount"), r'"Amount":\s*"(\d+\.?\d*)"', 1).cast("double")
)

# Add total_sales column
sales_data = sales_data.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))

# Add net_sale column
sales_data = sales_data.withColumn("net_sale", col("total_sales") - col("PromotionDiscountAmount"))

# Remove duplicates and filter out invalid sales
sales_data = sales_data.dropDuplicates(["OrderId"]) \
    .filter(col("net_sale") > 0)

# Write the transformed data to SQLite
sales_data.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:db.sqlite3") \
    .option("dbtable", "sales_data") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

# Perform data validation
sales_data.createOrReplaceTempView("sales_data")

# Count total records
total_records = spark.sql("SELECT COUNT(*) AS total_records FROM sales_data")
total_records.show()

# Total sales amount by region
total_sales_by_region = spark.sql("SELECT region, SUM(total_sales) AS total_sales FROM sales_data GROUP BY region")
total_sales_by_region.show()

# Average sales amount per transaction
avg_sales = spark.sql("SELECT AVG(net_sale) AS avg_sales FROM sales_data")
avg_sales.show()

# Ensure no duplicate OrderId values
duplicates = spark.sql("SELECT COUNT(OrderId) - COUNT(DISTINCT OrderId) AS duplicate_orders FROM sales_data")
duplicates.show()

print("Data processing completed successfully!")
