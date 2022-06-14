import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MyApp {
  def main (arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WASBIOTest")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/home/dell/sampleFile")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("/home/dell/OutputSampleFile")
    println(counts.count())
  }
}
