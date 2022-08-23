package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Testing2 {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .getOrCreate()

    val schema = new StructType()
      .add("studentid",IntegerType)
      .add("name",StringType)
      .add("dept",StringType)
      .add("subject",StringType)
      .add("marks",DoubleType)

    spark.sparkContext.setLogLevel("ERROR")

    val dataDir = "C:/yo/data"
    val archiveDir = "C:/Users/yogesh.ahire/Desktop/Study/archive"



    val df = spark
      .readStream
      .format("csv")
      .option("header","true")
      .schema(schema)
      .option("latestFirst","false")
      .option("spark.sql.streaming.fileSource.cleaner.numThreads", "2")
      .option("spark.sql.streaming.fileSource.log.compactInterval","1")
      .option("spark.sql.streaming.fileSource.log.cleanupDelay","1")
      .option("sourceArchiveDir",archiveDir)
      .option("cleanSource","archive")
      .load(dataDir)
      .writeStream
      .format("console")
      .outputMode("Append")
      //.format("csv")
      //.option("path","C:/Users/yogesh.ahire/Desktop/Study/dataWrite4")
      //.option("checkpointLocation", "C:/Users/yogesh.ahire/Desktop/Study/dataWrite4/checkpoint")
      .start()
      .awaitTermination()

  }
}
