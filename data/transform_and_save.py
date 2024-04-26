from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, floor, to_timestamp, date_format

# Create a Spark session
spark = SparkSession.builder.appName("ETL").getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 35)

# Read the CSV file into a DataFrame
df = spark.read.option("header", "true").csv("raw_data/Traffic_Crashes_Crashes*.csv")

# Select the columns you want to keep
selected_columns = [
    "CRASH_RECORD_ID",
    "CRASH_DATE",
    "POSTED_SPEED_LIMIT",
    "DEVICE_CONDITION",
    "WEATHER_CONDITION",
    "LIGHTING_CONDITION",
    "ROADWAY_SURFACE_COND",
    "ROAD_DEFECT",
    "INJURIES_TOTAL",
    "CRASH_HOUR",
    "CRASH_DAY_OF_WEEK",
    "CRASH_MONTH",
]

# Apply the selected columns to the DataFrame
df_transformed = df.select(selected_columns)

# Convert CRASH_DATE to timestamp format
df_transformed = df_transformed.withColumn(
    "CRASH_DATE",
    to_timestamp(col("CRASH_DATE"), "MM/dd/yyyy hh:mm:ss a")
)

# Format the timestamp to the desired string format
df_transformed = df_transformed.withColumn(
    "CRASH_DATE",
    date_format(col("CRASH_DATE"), "MM/dd/yyyy HH:mm:ss")
)

# Transform the DEVICE_CONDITION column
df_transformed = df_transformed.withColumn(
    "DEVICE_FAULTY",
    when(col("DEVICE_CONDITION") == "FUNCTIONING PROPERLY", 1)
    .when(col("DEVICE_CONDITION") == "UNKNOWN", 0)
    .otherwise(-1)
)

df_transformed = df_transformed.withColumn(
    "INJURIES",
    when(col("INJURIES_TOTAL") > 0, 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "ROAD_DEFECT_INT",
    when(col("ROAD_DEFECT") == "NO DEFECTS", -1)
    .when(col("ROAD_DEFECT") == "UNKNOWN", 0)
    .otherwise(1)
)

df_transformed = df_transformed.withColumn(
    "ROADWAY_DRY",
    when(col("ROADWAY_SURFACE_COND") == "DRY", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "ROADWAY_WET",
    when(col("ROADWAY_SURFACE_COND") == "WET", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "ROADWAY_ICE",
    when(col("ROADWAY_SURFACE_COND") == "ICE", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "ROADWAY_SNOW_SLUSH",
    when(col("ROADWAY_SURFACE_COND") == "SNOW OR SLUSH", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "DAYLIGHT",
    when(col("LIGHTING_CONDITION") == "DAYLIGHT", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "DUSK",
    when(col("LIGHTING_CONDITION") == "DUSK", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "DAWN",
    when(col("LIGHTING_CONDITION") == "DAWN", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "DARK_WITH_LIGHT",
    when(col("LIGHTING_CONDITION") == "DARKNESS, LIGHTED ROAD", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "DARK_NO_LIGHT",
    when(col("LIGHTING_CONDITION") == "DARKNESS", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "WEATHER_CLEAR",
    when(col("WEATHER_CONDITION") == "UNKNOWN", 1)
    .when(col("WEATHER_CONDITION") == "CLEAR", 1)
    .otherwise(0)
)

df_transformed = df_transformed.withColumn("POSTED_SPEED_LIMIT_INT", floor(col("POSTED_SPEED_LIMIT") / 10))

# Drop the DEVICE_CONDITION column if DEVICE_CONDITION_INT is storing the value
df_transformed = df_transformed.drop("DEVICE_CONDITION", "INJURIES_TOTAL", "ROAD_DEFECT", "ROADWAY_SURFACE_COND",
                                     "LIGHTING_CONDITION", "WEATHER_CONDITION")

# Write the transformed DataFrame to a new CSV file
df_transformed.write.mode("overwrite").option("header", "true").csv("cleaned_data")

# Stop the Spark session
spark.stop()
