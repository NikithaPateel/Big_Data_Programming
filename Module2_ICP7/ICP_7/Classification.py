from pyspark.sql import SparkSession
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import col
import sys
import os

os.environ["HADOOP_HOME"] = "C:\\Users\\nikit\\OneDrive\\Desktop\\winutils"
os.environ["SPARK_HOME"] = "C:\\Users\\nikit\\OneDrive\\Desktop\\spark-2.3.3"
import findspark
findspark.init()

import numpy as np
# Create spark session
spark = SparkSession.builder.appName("NB").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Load data and select feature and label columns
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("C:\\Users\\nikit\\PycharmProjects\\ICP_7\\adult.csv")
data.show()
data = data.withColumnRenamed("age", "label").select("label", col(" education-num").alias("education-num"), col(" hours-per-week").alias("hours-per-week"))
data = data.select(data.label.cast("double"), "education-num", "hours-per-week")
# Create vector assembler for feature columns
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)
# Split data into training and test data set
training, test = data.select("label", "features").randomSplit([0.85, 0.15])
# Create Navie Bayes model and fit the model with training dataset
nb = NaiveBayes()
model = nb.fit(training)
# Generate prediction from test dataset
predictions = model.transform(test)
# Evuluate the accuracy of the model
evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)
# Show model accuracy
print("Accuracy:", accuracy)
# Report
predictionAndLabels = predictions.select("label", "prediction").rdd
metrics = MulticlassMetrics(predictionAndLabels)
print("Confusion Matrix:", metrics.confusionMatrix())
print("Precision:", metrics.precision())
print("Recall:", metrics.recall())
print("F-measure:", metrics.fMeasure())