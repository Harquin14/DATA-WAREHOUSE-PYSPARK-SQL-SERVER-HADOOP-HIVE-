from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time
import traceback

# ==========================================================
# Initialize Spark Session
# ==========================================================
try:
    spark = (
        SparkSession.builder
        .appName("SilverLayerTransformation")
        .config("spark.jars", r"D:\spark_drivers\mssql-jdbc-13.2.1.jre11.jar")
        .getOrCreate()
    )

    print("✅ Spark session initialized successfully.")
except Exception as e:
    print("❌ Failed to initialize Spark session.")
    print(str(e))
    raise e

print("=" * 60)
print("Loading Silver Layer")
print("=" * 60)

# ==========================================================
# Define file paths
# ==========================================================
base_path = r"D:\ETL PySpark\Bronze"
bronze_paths = {
    "crm_cust_info": f"{base_path}/bronze_crm_cust_info",
    "crm_prd_info": f"{base_path}/bronze_crm_prd_info",
    "crm_sales_details": f"{base_path}/bronze_crm_sales_details",
    "erp_cust_az12": f"{base_path}/bronze_erp_cust_az12",
    "erp_loc_a101": f"{base_path}/bronze_erp_loc_a101",
    "erp_px_cat_g1v2": f"{base_path}/bronze_erp_px_cat_g1v2",
}

# ==========================================================
# Helper function for timing
# ==========================================================
def log_duration(table_name, start_time):
    duration = round(time.time() - start_time, 2)
    print(f">> {table_name} load duration: {duration} seconds")
    print("=" * 55)


# ==========================================================
# Load and Transform Each Bronze Table → Silver
# ==========================================================
try:
    # ================= CRM Customer Info =================
    print(">>>> Loading silver.crm_cust_info")
    start = time.time()

    bronze_cust = spark.read.parquet(bronze_paths["crm_cust_info"])

    window_spec = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
    silver_cust = (
        bronze_cust
        .withColumn("Srt", F.row_number().over(window_spec))
        .filter(F.col("Srt") == 1)
        .filter(F.col("cst_id").isNotNull())
        .withColumn("cst_firstname", F.trim(F.col("cst_firstname")))
        .withColumn("cst_lastname", F.trim(F.col("cst_lastname")))
        .withColumn(
            "cst_marital_status",
            F.when(F.upper(F.trim(F.col("cst_marital_status"))) == "S", "Single")
             .when(F.upper(F.trim(F.col("cst_marital_status"))) == "M", "Married")
             .otherwise("n/a")
        )
        .withColumn(
            "cst_gender",
            F.when(F.upper(F.trim(F.col("cst_gender"))) == "F", "Female")
             .when(F.upper(F.trim(F.col("cst_gender"))) == "M", "Male")
             .otherwise("n/a")
        )
        .select(
            "cst_id", "cst_key", "cst_firstname", "cst_lastname",
            "cst_marital_status", "cst_gender", "cst_create_date"
        )
    )

    silver_cust.write.mode("overwrite").parquet("D:/ETL PySpark/Silver/silver_crm_cust_info")
    log_duration("silver.crm_cust_info", start)

    # ================= CRM Product Info =================
    print(">>>> Loading silver.crm_prd_info")
    start = time.time()

    bronze_prd = spark.read.parquet(bronze_paths["crm_prd_info"])

    silver_prd = (
        bronze_prd
        .withColumn("cat_id", F.regexp_replace(F.substring("prd_key", 1, 5), "-", "_"))
        .withColumn("prd_key", F.substring("prd_key", 7, 100))
        .withColumn("prd_cost", F.coalesce(F.col("prd_cost"), F.lit(0)))
        .withColumn(
            "prd_line",
            F.when(F.upper(F.trim(F.col("prd_line"))) == "R", "Road")
             .when(F.upper(F.trim(F.col("prd_line"))) == "M", "Mountain")
             .when(F.upper(F.trim(F.col("prd_line"))) == "T", "Touring")
             .when(F.upper(F.trim(F.col("prd_line"))) == "S", "Other Sales")
             .otherwise("n/a")
        )
        .withColumn("prd_start_dt", F.to_date(F.col("prd_start_dt")))
    )

    w = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    silver_prd = silver_prd.withColumn("prd_end_dt", F.date_sub(F.lead("prd_start_dt").over(w), 1))

    silver_prd.write.mode("overwrite").parquet("D:/ETL PySpark/Silver/silver_crm_prd_info")
    log_duration("silver.crm_prd_info", start)

    # ================= CRM Sales Details =================
    print(">>>> Loading silver.crm_sales_details")
    start = time.time()

    bronze_sales = spark.read.parquet(bronze_paths["crm_sales_details"])

    silver_sales = (
        bronze_sales
        .withColumn("sls_order_dt",
                    F.when(
                        (F.length(F.col("sls_order_dt").cast("string")) != 8) |
                        (F.col("sls_order_dt").cast("string") == "0"),
                        None
                    ).otherwise(F.to_date(F.col("sls_order_dt").cast("string"), "yyyyMMdd")))
        .withColumn("sls_ship_dt",
                    F.when(
                        (F.length(F.col("sls_ship_dt").cast("string")) != 8) |
                        (F.col("sls_ship_dt").cast("string") == "0"),
                        None
                    ).otherwise(F.to_date(F.col("sls_ship_dt").cast("string"), "yyyyMMdd")))
        .withColumn("sls_due_date",
                    F.when(
                        (F.length(F.col("sls_due_date").cast("string")) != 8) |
                        (F.col("sls_due_date").cast("string") == "0"),
                        None
                    ).otherwise(F.to_date(F.col("sls_due_date").cast("string"), "yyyyMMdd")))
        .withColumn("sls_sales",
                    F.when(
                        (F.col("sls_sales").isNull()) |
                        (F.col("sls_sales") <= 0) |
                        (F.col("sls_sales") != F.col("sls_quantity") * F.abs(F.col("sls_price"))),
                        F.col("sls_quantity") * F.abs(F.col("sls_price"))
                    ).otherwise(F.col("sls_sales")))
        .withColumn("sls_price",
                    F.when((F.col("sls_price").isNull()) | (F.col("sls_price") <= 0),
                           F.col("sls_sales") / F.when(F.col("sls_quantity") != 0, F.col("sls_quantity")).otherwise(None))
                    .otherwise(F.col("sls_price")))
    )

    silver_sales.write.mode("overwrite").parquet("D:/ETL PySpark/Silver/silver_crm_sales_details")
    log_duration("silver.crm_sales_details", start)

    # ================= ERP Customer az12 =================
    print(">>>> Loading silver.erp_cust_az12")
    start = time.time()

    bronze_cust_az12 = spark.read.parquet(bronze_paths["erp_cust_az12"])

    silver_cust_az12 = (
        bronze_cust_az12
        .withColumn(
            "cid",
            F.when(F.col("cid").startswith("NAS"), F.expr("substring(cid, 4, length(cid))"))
             .otherwise(F.col("cid"))
        )
        .withColumn(
            "bdate",
            F.when(F.col("bdate") > F.current_date(), None).otherwise(F.col("bdate"))
        )
        .withColumn(
            "gen",
            F.when(F.upper(F.trim(F.col("gen"))).isin("M", "MALE"), "Male")
             .when(F.upper(F.trim(F.col("gen"))).isin("F", "FEMALE"), "Female")
             .otherwise("n/a")
        )
        .select("cid", "bdate", "gen")
    )

    silver_cust_az12.write.mode("overwrite").parquet("D:/ETL PySpark/Silver/silver_erp_cust_az12")
    log_duration("silver.erp_cust_az12", start)

    # ================= ERP loc a101 =================
    print(">>>> Loading silver.erp_loc_a101")
    start = time.time()

    bronze_loc = spark.read.parquet(bronze_paths["erp_loc_a101"])

    silver_loc = (
        bronze_loc
        .withColumn("cid", F.regexp_replace("cid", "-", ""))
        .withColumn("cntry",
                    F.when(F.trim(F.col("cntry")) == "DE", "Germany")
                     .when(F.trim(F.col("cntry")).isin("US", "USA"), "United States")
                     .when(F.trim(F.col("cntry")).isNull() | (F.trim(F.col("cntry")) == ""), "n/a")
                     .otherwise(F.trim(F.col("cntry"))))
    )

    silver_loc.write.mode("overwrite").parquet("D:/ETL PySpark/Silver/silver_erp_loc_a101")
    log_duration("silver.erp_loc_a101", start)

    # ================= ERP px_cat_g1v2 =================
    print(">>>> Loading silver.erp_px_cat_g1v2")
    start = time.time()

    bronze_cat = spark.read.parquet(bronze_paths["erp_px_cat_g1v2"])
    silver_cat = bronze_cat.select("id", "cat", "subcat", "maintenance")

    silver_cat.write.mode("overwrite").parquet("D:/ETL PySpark/Silver/silver_erp_px_cat_g1v2")
    log_duration("silver.erp_px_cat_g1v2", start)

    print("✅ Silver layer transformation completed successfully!")

except Exception as e:
    print("❌ ERROR OCCURRED DURING SILVER LAYER LOAD")
    print(traceback.format_exc())

finally:
    spark.stop()
    print("Spark session stopped.")