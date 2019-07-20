import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object graphframe {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val t = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\Module2_ICP6\\Source code\\Source code\\SparkGraphframe\\SparkGraphframe\\201508_trip_data.csv")

    val s = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\Module2_ICP6\\Source code\\Source code\\SparkGraphframe\\SparkGraphframe\\201508_station_data.csv")

    //Creating temp views Trips and stations
    t.createOrReplaceTempView("Trips")
    s.createOrReplaceTempView("Stations")

    //total no of stations
    val station = spark.sql("select * from Stations")
  //total trips
    val trips = spark.sql("select * from Trips")

    //removing duplicates
    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()
    //renaming columns
    val tripEdges = trips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")
    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    //show some vertices and edges
    stationGraph.vertices.show()

    stationGraph.edges.show()

  //Triangle Count
    val TriangCount = stationGraph.triangleCount.run()
    TriangCount.select("id","count").show()

    //Shortest paths
    val paths = stationGraph.shortestPaths.landmarks(Seq("MLK Library","Post at Kearney")).run()
   //paths.show()
     paths.show(numRows = 20,truncate = false)

    //pagerank
    // Run PageRank for a fixed number of iterations.
    val pagerankresults = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
    pagerankresults.vertices.show()
    pagerankresults.edges.show()

    stationGraph.vertices.write.csv("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\Module2_ICP6\\Source code\\Source code\\SparkGraphframe\\SparkGraphframe\\vertices")

    stationGraph.edges.write.csv("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\Module2_ICP6\\Source code\\Source code\\SparkGraphframe\\SparkGraphframe\\edges")


  }
}