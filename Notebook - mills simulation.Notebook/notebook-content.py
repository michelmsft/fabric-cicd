# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import random

# Create Spark session
spark = SparkSession.builder.appName("MillProductionSimulation").getOrCreate()

# Parameters
mills = ["MillA", "MillB", "MillC"]
products = ["Steel", "Aluminum", "Paper"]
num_records = 100

# Sample data generation
data = [
    (
        random.choice(mills),
        random.choice(products),
        random.randint(1000, 5000),  # quantity
        round(random.uniform(10.0, 50.0), 2),  # energy consumed (MWh)
        round(random.uniform(0.1, 2.0), 2),    # water usage (KL)
        round(random.uniform(0.5, 5.0), 2)     # emissions (tons CO2)
    )
    for _ in range(num_records)
]

schema = StructType([
    StructField("Mill", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("EnergyMWh", DoubleType(), True),
    StructField("WaterKL", DoubleType(), True),
    StructField("EmissionsTons", DoubleType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show simulated mill production
df.show(10, truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
