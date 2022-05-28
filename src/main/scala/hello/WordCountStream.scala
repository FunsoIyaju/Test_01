package hello

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object WordCountStream {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("WordCountStreamApp").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    val words = lines.select(explode(split(lines("value"), " ")).alias("word"))
    val count = words.groupBy("word").count()

    val query = count.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
