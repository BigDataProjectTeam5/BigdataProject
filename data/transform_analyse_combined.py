from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, floor, to_timestamp, date_format
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import IntegerType

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

# Select columns for analysis
features = [
    "DEVICE_FAULTY",
    "ROAD_DEFECT_INT",
    "ROADWAY_DRY",
    "ROADWAY_WET",
    "ROADWAY_ICE",
    "ROADWAY_SNOW_SLUSH",
    "DAYLIGHT",
    "DUSK",
    "DAWN",
    "DARK_WITH_LIGHT",
    "DARK_NO_LIGHT",
    "WEATHER_CLEAR",
    "POSTED_SPEED_LIMIT_INT"
]

selected_columns = features + ["INJURIES"]

data = df_transformed.select(selected_columns)

# Convert string columns to integer type
data = data.withColumn("DEVICE_FAULTY", data["DEVICE_FAULTY"].cast(IntegerType()))
data = data.withColumn("ROAD_DEFECT_INT", data["ROAD_DEFECT_INT"].cast(IntegerType()))
data = data.withColumn("ROADWAY_DRY", data["ROADWAY_DRY"].cast(IntegerType()))
data = data.withColumn("ROADWAY_WET", data["ROADWAY_WET"].cast(IntegerType()))
data = data.withColumn("ROADWAY_ICE", data["ROADWAY_ICE"].cast(IntegerType()))
data = data.withColumn("ROADWAY_SNOW_SLUSH", data["ROADWAY_SNOW_SLUSH"].cast(IntegerType()))
data = data.withColumn("DAYLIGHT", data["DAYLIGHT"].cast(IntegerType()))
data = data.withColumn("DUSK", data["DUSK"].cast(IntegerType()))
data = data.withColumn("DAWN", data["DAWN"].cast(IntegerType()))
data = data.withColumn("DARK_WITH_LIGHT", data["DARK_WITH_LIGHT"].cast(IntegerType()))
data = data.withColumn("DARK_NO_LIGHT", data["DARK_NO_LIGHT"].cast(IntegerType()))
data = data.withColumn("WEATHER_CLEAR", data["WEATHER_CLEAR"].cast(IntegerType()))
data = data.withColumn("POSTED_SPEED_LIMIT_INT", data["POSTED_SPEED_LIMIT_INT"].cast(IntegerType()))
data = data.withColumn("INJURIES", data["INJURIES"].cast(IntegerType()))

# Assemble features
assembler = VectorAssembler(
    inputCols=features,
    outputCol="features"
)

data = assembler.transform(data)

# Split the data into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=12345)

# Create a logistic regression model
lr = LogisticRegression(featuresCol="features", labelCol="INJURIES")

# Fit the model to the training data
lr_model = lr.fit(train_data)
training_summary = lr_model.summary
print("Summary:", training_summary)

# Make predictions on the test data
predictions = lr_model.transform(test_data)

# Get some insights into the crash data
#predictions.filter((col("INJURIES") != 0) | (col("prediction") != 0)).select("INJURIES", "prediction").show()

# Evaluate the model
evaluator = BinaryClassificationEvaluator(labelCol="INJURIES")
accuracy = evaluator.evaluate(predictions)

print("Accuracy:", accuracy)  # Get the coefficients and intercept of the model
coefficients = lr_model.coefficients
intercept = lr_model.intercept

# Get the area under the ROC curve
print("Area Under ROC:", training_summary.areaUnderROC)

print("Coefficients")
coefficients_dict = dict(zip(features, coefficients))
for feature, coef in coefficients_dict.items():
    print(f"{feature}: {coef}")

print("Intercept:", intercept)
# Stop the Spark session
spark.stop()
