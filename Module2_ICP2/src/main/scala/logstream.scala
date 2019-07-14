
import org.apache.spark._
import org.apache.spark.streaming.__



object logstream {

  def main(args: Array[String]): Unit = {


    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    val conf = new SparkConf().setMaster("local[2]").setAppName("log")

    //the checkpointed data would be rewritten every 10 seconds......checkpoint is nothing but cache but it stores on disk
    val ssc = new StreamingContext(conf, Seconds(5))

    //textFileStream can only monitor a folder when the files in the folder are being added or updated.
    val lines = ssc.textFileStream("logs")

    val wc = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    println(lines)
    wc.print()
    ssc.start()
    ssc.awaitTermination()


  }
}
