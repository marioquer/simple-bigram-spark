import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {
  val pathPrefix = "src/main/files/"

  def getSparkSession(isLocal: Boolean, appName: String = "Spark"): SparkSession = {
    val conf = new SparkConf().setAppName(appName)
    if (isLocal) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder.config(conf).getOrCreate()
    spark
  }
}
