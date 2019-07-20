import os
import findspark
findspark.init()

os.environ["HADOOP_HOME"] = "C:\\Users\\nikit\\OneDrive\\Desktop\\winutils"

from pyspark import rdd
from pyspark.python.pyspark.shell import spark
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("C:\\Users\\nikit\\PycharmProjects\\Lab2\\fifa-world-cup\\WorldCups.csv", 1)
    header = lines.first()
    content = lines.filter(lambda line: line != header)
    # creating an RDD
    rdd = content.map(lambda line: (line.split(","))).collect()
    #print(rdd)
    # no. of Columns
    rdd_len = content.map(lambda line: len(line.split(","))).distinct().collect()
    print(rdd_len)
    print("---------- 1. Venues & goals scored ------------")
    # venue - hosted country with highest goals (From RDD)
    rdd1 = (content.filter(lambda line: line.split(",")[6] != "NULL")
    .map(lambda line: (line.split(",")[1], int(line.split(",")[6])))
    .takeOrdered(10, lambda x : -x[1]))
   # print(rdd1)

    schema = StructType([StructField('Year', StringType(), True),
                         StructField('Country', StringType(), True),
                         StructField('Winner', StringType(), True),
                         StructField('Runners-Up', StringType(), True),
                         StructField('Third', StringType(), True),
                         StructField('Fourth', StringType(), True),
                         StructField('GoalsScored', StringType(), True),
                         StructField('QualifiedTeams', StringType(), True),
                         StructField('MatchesPlayed', StringType(), True),
                         StructField('Attendance', StringType(), True)])
    # Create data frame from the RDD


    years = ["1930", "1950", "1970", "1990", "2010"]
    (content.filter(lambda line: line.split(",")[0] in years)
     .map(lambda line: (line.split(",")[0], line.split(",")[2], line.split(",")[3])).collect())