from pyspark.sql import SparkSession

# -----------------------------------------
# 1. Initialize Spark session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("Query_Silver_Customer_Info") \
    .config("spark.sql.warehouse.dir", r"D:\ETL_PySpark\warehouse") \
    .config("spark.hadoop.io.nativeio.check.native", "false") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .enableHiveSupport() \
    .getOrCreate()

print(f"✅ Spark session started (Spark version: {spark.version})")

# -----------------------------------------
# 2. Try querying the Hive table
# -----------------------------------------
try:
    tables = spark.sql("SHOW TABLES IN silver").collect()
    table_names = [t.tableName for t in tables]
    print(f"📋 Tables in 'silver' database: {table_names}")

    if "crm_cust_info" in table_names:
        print("✅ Found Hive table: silver.crm_cust_info")
        df = spark.sql("SELECT * FROM silver.crm_cust_info LIMIT 10")
        df.show()
    else:
        raise Exception("Table not found in Hive")
except Exception as e:
    print(f"⚠️ Hive table not found or query failed ({e}). Trying Parquet path instead...")

    # -----------------------------------------
    # 3. Try reading from Parquet path
    # -----------------------------------------
    silver_path = r"D:\ETL_PySpark\silver\crm_cust_info"
    try:
        df = spark.read.parquet(silver_path)
        print(f"✅ Loaded data from Parquet: {silver_path}")
        df.show(10)
    except Exception as e2:
        print(f"❌ Failed to load from Parquet. Error:\n{e2}")

# -----------------------------------------
# 4. Stop Spark
# -----------------------------------------
spark.stop()
print("✅ Spark session stopped.")
