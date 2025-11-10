
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SQLServerConnectionTest") \
    .config("spark.jars", r"D:\spark_drivers\mssql-jdbc-13.2.1.jre11.jar") \
    .getOrCreate()

# Connection details
server = "localhost\\SQLEXPRESS"  # Use the instance name you confirmed
database = "WAREHOUSE_PSK"
username = "spark_user"
password = "Owusuansah1$$"

# JDBC URL
url = f"jdbc:sqlserver://{server}:1433;databaseName={database};encrypt=false;"

# Read data from one of your tables
df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "bronze.crm_cust_info") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

df.show()

spark.stop()

print("✅ Successfully connected to SQL Server and retrieved data!")