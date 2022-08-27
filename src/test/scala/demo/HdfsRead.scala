package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object HdfsRead {

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

    val df = spark
      .readStream
      .format("csv")
      .option("inferSchema","true")
      .option("header","true")
      .option("compression","gzip")
      .schema(schema)
      .load("C:/yo/data");

    df.printSchema()

    //hdfs://DEVupgrade/dinesh

     val df1 = df.select("studentid","name","marks","dept","subject").where("marks > 50  and marks < 80")
     val df2 = df.select("studentid","name","marks","dept","subject").where("marks > 80")

    val query = df1.writeStream.format("console").trigger(Trigger.ProcessingTime(50.seconds)).start()
    val query1 = df2.writeStream.format("console").trigger(Trigger.ProcessingTime(50.seconds)).start()

    /* val query1 = df2.writeStream
      .format("console")
      .format("csv")
      .option("path","C:/Users/yogesh.ahire/Desktop/Study/dataWrite")
      .option("checkpointLocation", "C:/Users/yogesh.ahire/Desktop/Study/dataWrite")
      .start()
*/
     query.awaitTermination()
     query1.awaitTermination()

  }

}
