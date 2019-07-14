import org.apache.spark.{SparkConf, SparkContext}

object Mergesort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\nikit\\OneDrive\\Desktop\\winutils" )
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list = List(10,7,5,8,4,3,1,2)
    def mergeSort(a: List[Int]): List[Int] = {
      val n = a.length / 2
      if (n == 0) a
      else {
        def merge(a: List[Int], b: List[Int]): List[Int] =
          (a, b) match {
            case(Nil, b) => b
            case(a, Nil) => a
            case(x :: as, y :: bs) =>
              if (x < y) x::merge(as, b)
              else y :: merge(a, bs)
          }
        val (left, right) = a splitAt(n)
        merge(mergeSort(left), mergeSort(right))
      }
    }
    val result=mergeSort(list)
    print(result)

  }
}