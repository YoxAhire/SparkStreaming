package demo

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object ArchiveStrategy extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkByExample")
    .config("spark.sql.shuffle.partition",2)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val dataDir = "C:\\practice\\dataA"
  val archiveDir = "C:\\practice\\archive"
  FileUtils.deleteDirectory(new File(dataDir))
  new Thread(new FileGenerator(dataDir)).start()

  val  writeQuery = spark.readStream
    .option("sourceArchiveDir", archiveDir)
    .option("cleanSource", "archive")
    .text(dataDir)
    .writeStream
    .format("console")
    .option("checkpointLocation", "C:\\Users\\yogesh.ahire\\Desktop\\Study\\data\\checkpointing1")
    .option("truncate",false)

  writeQuery.start().awaitTermination()
  FileUtils.deleteDirectory(new  File(dataDir))
}
