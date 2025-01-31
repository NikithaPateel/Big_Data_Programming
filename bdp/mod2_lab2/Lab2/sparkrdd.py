import os

os.environ["HADOOP_HOME"] = "C:\\Users\\nikit\\OneDrive\\Desktop\\winutils"
os.environ["SPARK_HOME"] = "C:\\Users\\nikit\\OneDrive\\Desktop\\spark-2.4.3"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import rdd

spark = SparkSession \
    .builder \
    .appName("Pyspark SQL Lab2") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

from operator import add

# 1 - Create Spark DataFrames
resultsDf = spark.read.load("C:\\Users\\nikit\\PycharmProjects\\Lab2\\fifa-world-cup\\WorldCups.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
matchesDf = spark.read.load("C:\\Users\\nikit\\PycharmProjects\\Lab2\\fifa-world-cup\\WorldCupMatches.csv",
                     format="csv", sep=",", inferSchema="true", header="true")


resultsDf.createOrReplaceTempView("results")
matchesDf.createOrReplaceTempView("matches")

#Total football worldcups

total = resultsDf.count()
print("\n number of football worldcups = " +str(total))

# countries winning world cups in desc order
print("\n number of countries winning word cups")
resultsDf.groupBy("Winner").count().orderBy('count',ascending= False).show()

# Year when hosting country won the world cup final
print("\nYear when hosting country won the final")
spark.sql("SELECT year, country, winner FROM results where Country = Winner").show()

# Average goals scored per match by year
print("\nAverage goal scored per match by year")
spark.sql("SELECT year, CEILING(GoalsScored/MatchesPlayed) avg_goals_per_match FROM results").show()

# Match with highest attendance
print("\nMatch with highest attendance")
spark.sql("SELECT Year, Datetime, Stadium, City, `Home Team Name`, `Away Team Name`, Attendance FROM matches "
          "WHERE Attendance = (SELECT MAX(Attendance) FROM matches)").show()

# Match with most number of goals
print("\nMatch with most number of goals")
spark.sql("SELECT Year, Datetime, City, `Home Team Name`, `Away Team Name`, "
          "(`Home Team Goals` + `Away Team Goals`) total_goals "
          "FROM matches "
          "ORDER BY total_goals DESC").show()

# Country with most final wins
print("\nCountry with most final wins")
resultsDf.groupBy(['Winner']).count().orderBy("count", ascending=False).show(1)

# All Participating countries in World cup
print("\nAll Participating countries in World cup")
spark.sql("SELECT `Home Team Name` country FROM matches "
          "UNION "
          "SELECT `Away Team Name` country FROM matches "
          "ORDER BY 1").show(100)

# Top 5 cities with most number of matches played
print("\nTop 5 cities with most number of matches played")
matchesDf.groupBy(['City']).count().orderBy("count", ascending=False).show(5)

# Top 5 interesting final matches
print("\nTop 5 interesting final matches")
spark.sql("SELECT Year, `Home Team Name` team1, `Home Team Goals` team1score,"
          " `Away Team Name` team2, `Away Team Goals` team2score, `Home Team Goals`+`Away Team Goals` total"
          " FROM matches WHERE Stage = 'Final'"
          "ORDER BY 6 DESC").show(5)

#Perform 5 queries using spark rdds

print("\n queries using spark rdds")

# Convert df to rdd
matchesRdd = matchesDf.rdd
resultsRdd = resultsDf.rdd

# Count no. of world cups
print("\nNumber of world cups = " + str(resultsRdd.count()))

# Countries winning in world cup final
print("\nCountries winning in world cup final")
print(resultsRdd.map(lambda x: x[2]).distinct().collect())

# Countries played in world cup final but never won
win = resultsRdd.map(lambda x: x[2]).distinct().collect()
lose = resultsRdd.map(lambda x: x[3]).distinct().collect()
print("\nCountries played in world cup final but never won")
print(list(set(lose) - set(win)))

# Hosting cities
print("\nHosting cities")
print(matchesRdd.map(lambda x: (x[4], 1)).reduceByKey(add).collect())

# Number of times brazil won the world cup
print("\nNumber of times brazil won the world cup = " + str(resultsRdd.filter(lambda x: x[2] == 'Brazil').count()))

