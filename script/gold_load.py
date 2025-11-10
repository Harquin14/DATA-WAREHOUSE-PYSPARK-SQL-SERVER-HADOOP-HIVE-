from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time
import traceback
import os

# ==========================================================
# Initialize Spark Session
# ==========================================================
try:
    spark = (
        SparkSession.builder
        .appName("GoldLayerTransformation")
        .config("spark.jars", r"D:\spark_drivers\mssql-jdbc-13.2.1.jre11.jar")
        .getOrCreate()
    )
    print("✅ Spark session initialized successfully.")
except Exception as e:
    print("❌ Failed to initialize Spark session.")
    raise e

print("=" * 70)
print("Building Gold Layer (Star Schema)")
print("=" * 70)

# ==========================================================
# JDBC Configuration (SQL Server)
# ==========================================================
jdbc_url = "jdbc:sqlserver://localhost\\SQLEXPRESS:1433;databaseName=WAREHOUSE_PSK;encrypt=false"
db_properties = {
    "user": "spark_user",
    "password": "Owusuansah1$$",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# ==========================================================
# Paths
# ==========================================================
silver_path = r"D:/ETL PySpark/Silver"
gold_path = r"D:/ETL PySpark/Gold"

# Create gold directory if missing
os.makedirs(gold_path, exist_ok=True)

silver_tables = {
    "crm_cust_info": f"{silver_path}/silver_crm_cust_info",
    "crm_prd_info": f"{silver_path}/silver_crm_prd_info",
    "crm_sales_details": f"{silver_path}/silver_crm_sales_details",
    "erp_cust_az12": f"{silver_path}/silver_erp_cust_az12",
    "erp_loc_a101": f"{silver_path}/silver_erp_loc_a101",
    "erp_px_cat_g1v2": f"{silver_path}/silver_erp_px_cat_g1v2"
}

# ==========================================================
# Helper Function for Load Duration
# ==========================================================
def log_duration(table_name, start_time, destination):
    duration = round(time.time() - start_time, 2)
    print(f"✅ {table_name} written to {destination} in {duration} seconds")
    print("-" * 60)

# ==========================================================
# 1️⃣ DIM_CUSTOMERS
# ==========================================================
try:
    print(">>>> Building gold.dim_customers")
    start = time.time()

    ci = spark.read.parquet(silver_tables["crm_cust_info"])
    ca = spark.read.parquet(silver_tables["erp_cust_az12"])
    l = spark.read.parquet(silver_tables["erp_loc_a101"])

    dim_customers = (
        ci
        .join(ca, ci.cst_key == ca.cid, "left")
        .join(l, ci.cst_key == l.cid, "left")
        .withColumn(
            "gender",
            F.when(F.col("cst_gender") != "n/a", F.col("cst_gender"))
             .otherwise(F.coalesce(F.col("gen"), F.lit("n/a")))
        )
        .withColumn("customer_key", F.row_number().over(Window.orderBy("cst_id")))
        .select(
            "customer_key",
            F.col("cst_id").alias("customer_id"),
            F.col("cst_key").alias("customer_number"),
            F.col("cst_firstname").alias("first_name"),
            F.col("cst_lastname").alias("last_name"),
            F.col("cntry").alias("country"),
            F.col("bdate").alias("birthdate"),
            "gender",
            F.col("cst_marital_status").alias("marital_status"),
            F.col("cst_create_date").alias("create_date")
        )
    )

    # Write to local Parquet
    dim_customers.write.mode("overwrite").parquet(f"{gold_path}/gold_dim_customers")
    log_duration("gold.dim_customers", start, "Parquet")

    # Write to SQL Server
    start_sql = time.time()
    dim_customers.write.mode("overwrite").jdbc(jdbc_url, "gold.dim_customers", properties=db_properties)
    log_duration("gold.dim_customers", start_sql, "SQL Server")

except Exception:
    print("❌ Error while building gold.dim_customers")
    print(traceback.format_exc())

# ==========================================================
# 2️⃣ DIM_PRODUCTS
# ==========================================================
try:
    print(">>>> Building gold.dim_products")
    start = time.time()

    pi = spark.read.parquet(silver_tables["crm_prd_info"])
    pc = spark.read.parquet(silver_tables["erp_px_cat_g1v2"])

    dim_products = (
        pi.join(pc, pi.cat_id == pc.id, "left")
        .filter(F.col("prd_end_dt").isNull())
        .withColumn("product_key", F.row_number().over(Window.orderBy("prd_id", "prd_start_dt")))
        .select(
            "product_key",
            F.col("prd_id").alias("product_id"),
            F.col("prd_key").alias("product_number"),
            F.col("prd_nm").alias("product_name"),
            F.col("cat_id").alias("category_id"),
            "cat", "subcat", "maintenance",
            F.col("prd_cost").alias("cost"),
            F.col("prd_line").alias("product_line"),
            F.col("prd_start_dt").alias("start_date")
        )
    )

    # Write to local Parquet
    dim_products.write.mode("overwrite").parquet(f"{gold_path}/gold_dim_products")
    log_duration("gold.dim_products", start, "Parquet")

    # Write to SQL Server
    start_sql = time.time()
    dim_products.write.mode("overwrite").jdbc(jdbc_url, "gold.dim_products", properties=db_properties)
    log_duration("gold.dim_products", start_sql, "SQL Server")

except Exception:
    print("❌ Error while building gold.dim_products")
    print(traceback.format_exc())

# ==========================================================
# 3️⃣ FACT_SALES
# ==========================================================
try:
    print(">>>> Building gold.fact_sales")
    start = time.time()

    sd = spark.read.parquet(silver_tables["crm_sales_details"])
    dim_prod = spark.read.parquet(f"{gold_path}/gold_dim_products")
    dim_cust = spark.read.parquet(f"{gold_path}/gold_dim_customers")

    fact_sales = (
        sd.join(dim_prod, sd.sls_prd_key == dim_prod.product_number, "left")
          .join(dim_cust, sd.sls_cust_id == dim_cust.customer_id, "left")
          .select(
              F.col("sls_ord_num").alias("order_number"),
              F.col("product_key"),
              F.col("customer_key"),
              F.col("sls_order_dt").alias("order_date"),
              F.col("sls_ship_dt").alias("shipping_date"),
              F.col("sls_due_date").alias("due_date"),
              F.col("sls_sales").alias("sales_amount"),
              F.col("sls_quantity").alias("quantity"),
              F.col("sls_price").alias("price")
          )
    )

    # Write to local Parquet
    fact_sales.write.mode("overwrite").parquet(f"{gold_path}/gold_fact_sales")
    log_duration("gold.fact_sales", start, "Parquet")

    # Write to SQL Server
    start_sql = time.time()
    fact_sales.write.mode("overwrite").jdbc(jdbc_url, "gold.fact_sales", properties=db_properties)
    log_duration("gold.fact_sales", start_sql, "SQL Server")

except Exception:
    print("❌ Error while building gold.fact_sales")
    print(traceback.format_exc())

finally:
    spark.stop()
    print("✅ Spark session stopped successfully.")