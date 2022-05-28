package hello

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    //val inputPath = args(0)
    //val outputPath = args(1)

    val inputPath = "D:\\Data\\in\\README.txt"
    val outputPath = "D:\\Data\\out"
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCountApp")
    val sc = new SparkContext(config)
    val lines = sc.textFile(inputPath)
    val wordCounts = lines.flatMap { line => line.split(" ") }
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordCounts.saveAsTextFile(outputPath)
  }
}
