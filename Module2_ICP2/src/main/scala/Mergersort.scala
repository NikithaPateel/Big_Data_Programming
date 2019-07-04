import org.apache.spark.{SparkConf, SparkContext}

object Mergesort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\nikit\\OneDrive\\Desktop\\winutils" )
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list = List(10,7,5,8,4,3,1,2)
    def mergeSort(xs: List[Int]): List[Int] = {
      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]): List[Int] =
          (xs, ys) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(x :: xs1, y :: ys1) =>
              if (x < y) x::merge(xs1, ys)
              else y :: merge(xs, ys1)
          }
        val (left, right) = xs splitAt(n)
        merge(mergeSort(left), mergeSort(right))
      }
    }
    val result=mergeSort(list)
    print(result)

  }
}