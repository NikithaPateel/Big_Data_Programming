import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._

object secondarysort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\nikit\\OneDrive\\Desktop\\winutils")
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("input.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => (k(0), k(1)) }

    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(_.toList.sortBy(r => r))


    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    resultRDD.saveAsTextFile(path= "output1")
  }
}