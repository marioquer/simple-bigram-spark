import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import sys

if __name__ == "__main__":
    conf=SparkConf().setAppName("wordcount").setMaster("local[*]")
    sc=SparkContext.getOrCreate(conf)

    text_file = sc.textFile(sys.argv[1])
    counts = text_file.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile("files/wordcount")
    sc.stop()
