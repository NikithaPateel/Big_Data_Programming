import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


object icp3 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\nikit\\OneDrive\\Desktop\\winutils" )
    val conf = new SparkConf().setAppName("icp3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
import sqlcontext.implicits._

//Import the dataset and create data frames directly on import
    val df = sqlcontext.read.format("csv")
      .option("delimiter",",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\nikit\\OneDrive\\Desktop\\CSEE-5590\\Big_Data_Programming\\m1-icp3\\survey.csv")

   // df.show()
    //save data to file
    println("save data to a file")
//    df.write.saveAsTable("data")

  //union
    val df1 = df.limit(10)
    val df2 = df.limit(10)

    val uniondf = df1.union(df2)

    val sortdf = uniondf.orderBy("Country")
    //sortdf.show()
//group by
    df.createOrReplaceTempView("country")
    val treatment = sqlcontext.sql("select count(Country),treatment from country group by treatment")
   // treatment.show()

    val df3 = df.select("Country","state","Age","Gender","Timestamp")
    val df4 = df.select("self_employed","treatment","family_history","Timestamp")

    df3.createOrReplaceTempView( "table1")
    df4.createOrReplaceTempView("table2")


    val joindf = sqlcontext.sql("select table1.Gender,table1.Country,table2.treatment,table2.family_history FROM table1 Join table2 on table2.Timestamp = table1.timestamp")
    //joindf.show()

    val mindf = sqlcontext.sql("select Min(Age) from table1")
    //mindf.show()

    val avgdf = sqlcontext.sql("select Avg(Age) from table1")
    //avgdf.show()

    val dupdf = df.dropDuplicates("Country")
    dupdf.show()
    println("count of dup data" +dupdf.count())
  //save to a file
    //dupdf.write.parquet("data")
    dupdf.coalesce(1).write.option("header","true").format("csv").save("output")

  }
}