import org.apache.spark.sql.SparkSession

object Mergesort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\nikit\\OneDrive\\Desktop\\winutils" )
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.csv("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\m1-icp3\\survey.csv")
    df.show()

  }
}