import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{Args,BeforeAndAfterAll, Suite, Status}

trait SparkTest extends BeforeAndAfterAll with Suite {

  val master = "local[2]"
  val appName = "testing-spark-process"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName(appName)
      .set("spark.io.compression.codec", "snappy")

    sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

  }

  override def afterAll(): Unit = {
    sparkSession.stop
  }

  abstract override def run(testName: Option[String], args: Args): Status = super.run(testName, args)

}
