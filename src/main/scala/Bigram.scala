import scala.util.control.Breaks._
import java.io.File
import java.io.PrintWriter

object Bigram {

  def calculateBigram(inputPath: String, outputPath: String, isLocal: Boolean): Unit = {
    val spark = SparkUtils.getSparkSession(isLocal)
    val sc = spark.sparkContext
    val inputRDD = sc.textFile(inputPath)
    // delete punctuation, special characters
    val words = inputRDD.flatMap(line => line.split(" "))
      .map(word => word.replaceFirst("^[^a-zA-Z]+", "").replaceAll("[^a-zA-Z]+$", "").toLowerCase())
      .filter(word => word.length() > 0)
      .zipWithIndex()
      .map(_.swap)

    val shifted = words.map { case (k, v) => (k - 1, v) }
      .filter(_._1 >= 0)

    // total number of bigram
    val bigramCount = shifted.count()

    val histogram = words.join(shifted).map { case (_, (v1, v2)) => ((v1, v2), 1) }.reduceByKey(_ + _)

    val distinctCount = histogram.count()
    val sortedHistogram = histogram.sortBy(_._2, ascending = false)
    var tenPercentage = 0
    var i = 1
    breakable {
      while (true) {
        tenPercentage = sortedHistogram.take(i).reduce((a1, a2) => (("", ""), a1._2 + a2._2))._2
        if (tenPercentage >= 0.1 * bigramCount) {
          break
        }
        i = i + 1
      }
    }

    val result: String = distinctCount + "\n" + sortedHistogram.first()._1 + "\n" + i
    println(result)

    val writer = new PrintWriter(new File(outputPath))
    writer.write(result)
    writer.close()
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    calculateBigram(SparkUtils.pathPrefix + "const.txt", SparkUtils.pathPrefix + "bigram_output.txt", true)
  }
}
