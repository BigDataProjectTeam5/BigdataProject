from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("LogisticRegressionExample").getOrCreate()

# Load the cleaned data
cleaned_data = spark.read.option("header", "true").csv("cleaned_data")


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

data = cleaned_data.select(selected_columns)

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
