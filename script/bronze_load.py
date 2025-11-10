from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import os

# -----------------------------
# 1️⃣ Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Bronze_Layer_Loader") \
    .config("spark.jars", r"D:\spark_drivers\mssql-jdbc-13.2.1.jre11.jar") \
    .config("spark.sql.warehouse.dir", "D:/ETL_PySpark/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("✅ Spark session initialized successfully.\n")

# -----------------------------
# 2️⃣ Define Connection Settings
# -----------------------------
jdbc_url = "jdbc:sqlserver://localhost\\SQLEXPRESS:1433;databaseName=WAREHOUSE_PSK;encrypt=false"
username = "spark_user"
password = "Owusuansah1$$"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

bronze_base_path = "file:///D:/ETL_PySpark/Bronze"


# Ensure base folder exists
os.makedirs(r"D:\ETL PySpark\Bronze", exist_ok=True)

# -----------------------------
# 3️⃣ Tables to Ingest
# -----------------------------
tables = [
    "bronze.crm_cust_info",
    "bronze.crm_prd_info",
    "bronze.crm_sales_details",
    "bronze.erp_cust_az12",
    "bronze.erp_loc_a101",
    "bronze.erp_px_cat_g1v2"
]

# -----------------------------
# 4️⃣ Load Each Table with Try/Catch
# -----------------------------
for table_name in tables:
    print(f" Reading table: {table_name}")

    try:
        # Read from SQL Server
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

        print(f"✅ Successfully read table: {table_name} | Rows: {df.count()}")

        # Save path
        clean_name = table_name.replace(".", "_")
        save_path = f"{bronze_base_path}/{clean_name}"

        # Write to Bronze Layer
        df.write.mode("overwrite").parquet(save_path)
        print(f"✅ Bronze data written successfully to: {save_path}\n")

        # Register as external Hive table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING PARQUET
            LOCATION '{save_path}'
        """)
        print(f"✅ External Hive table created: {table_name}\n")

    except AnalysisException as ae:
        print(f" Analysis error for {table_name}: {ae}\n")

    except Exception as e:
        print(f"❌ Failed to process {table_name}: {e}\n")

# -----------------------------
# 5️⃣ Wrap up
# -----------------------------
print(" Bronze layer load completed for all available tables.")
spark.stop()