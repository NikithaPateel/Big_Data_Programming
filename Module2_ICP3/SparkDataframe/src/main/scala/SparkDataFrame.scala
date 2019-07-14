import org.apache.spark.sql.SparkSession

object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\\\nikit\\\\OneDrive\\\\Desktop\\\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.json("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\Module2_ICP3\\survey.csv")
    df.show()
  }
}
