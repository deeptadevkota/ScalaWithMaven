import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import EventManager.EventManager



object MyApp {
  // $SPARK_HOME/bin/spark-submit --class MyApp --master local SparkSimpleApp-1.0-SNAPSHOT-jar-with-dependencies.jar
  // mvn assembly:single
  def main (arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyApp")
    val sc = new SparkContext(conf)
    var em: EventManager = null
    val HTTP_endpoint = System.getProperty("HTTP_endpoint")
    if(HTTP_endpoint ==null)
    {
      em = new EventManager(sc.appName, sc.applicationId)
    }
    else
    {
      em = new EventManager( sc.appName, sc.applicationId, HTTP_endpoint)
    }
    sc.addSparkListener(em)
    sc.setLogLevel("ERROR")

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

  }
}
