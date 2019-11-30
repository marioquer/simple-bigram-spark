object WordCount {
  def calculateWordCount(inputPath: String, outputPath: String, isLocal: Boolean): Unit = {
    /**
      * reference from https://spark.apache.org/examples.html
      */
    val spark = SparkUtils.getSparkSession(isLocal)
    val sc = spark.sparkContext
    val textFile = sc.textFile(inputPath)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(outputPath)
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    calculateWordCount(SparkUtils.pathPrefix + "const.txt", SparkUtils.pathPrefix + "wordcount_output", true)
  }
}
