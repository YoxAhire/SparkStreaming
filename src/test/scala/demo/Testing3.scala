package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CleanSourceMode.ARCHIVE
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

import scala.concurrent.duration.DurationInt

object Testing3 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .config("spark.sql.shuffle.partition",2)
      .getOrCreate()

    val schema = new StructType()
      .add("studentid",IntegerType)
      .add("name",StringType)
      .add("dept",StringType)
      .add("subject",StringType)
      .add("marks",DoubleType)

    val dataDir = "C:/yo/data/data-archival-strategy"

    spark.sparkContext.setLogLevel("ERROR")
    spark
      .readStream
      .format("csv")
      .schema(schema)
      .option("spark.sql.streaming.fileSource.log.compactInterval","1")
      .option("spark.sql.streaming.fileSource.log.cleanupDelay","1")
      .option(
        "sourceArchiveDir",
        "C:/yo/data/data-archival")
      .option("cleanSource","ARCHIVE" )
      .option("latestFirst","false")
      .option("spark.sql.streaming.fileSource.cleaner.numThreads", "10")
      .option("header", "true")
      .load(dataDir)
      .withColumn("date", lit("2022-08-23"))
      .writeStream
      .outputMode(OutputMode.Append)
      .trigger(Trigger.ProcessingTime(50.seconds))
      .format("console")
      .start()
      .awaitTermination()
  }
}
