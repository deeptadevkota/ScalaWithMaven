import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import EventManager.EventManager
import scalaj.http._


object MyApp {
  // $SPARK_HOME/bin/spark-submit --class MyApp --master local SparkSimpleApp-1.0-SNAPSHOT-jar-with-dependencies.jar
  // mvn assembly:single
  def main (arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyApp")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val em = new EventManager(sc.applicationId, sc.appName)
    sc.addSparkListener(em)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Application with Listener")
      .getOrCreate()


    import spark.implicits._
    val person = Seq(
      ("John", "Barcelona"),
      ("Naveen", "Texas"),
      ("Rahul", "Bangalore")
    ).toDF("Name", "City")

    val city = Seq(
      ("Barcelona", "Spain", "Euro"),
      ("Bangalore", "India", "INR")
    ).toDF("City", "Country", "Currency")

    person.join(
      city,
      person("city") <=> city("city")
    ).show()

    val sort: Option[String] = None
    val query = "http language:scala"

    // the `query` parameter is automatically url-encoded
    // `sort` is removed, as the value is not defined
    val response: HttpResponse[String] = Http("http://www.google.com").asString
    println(s"${response.headers}")


    println("application ended - scalaj")
  }
}
