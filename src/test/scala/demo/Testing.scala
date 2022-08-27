package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Testing {

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
      .schema(schema)
      .load("C:/yo/data");

    df.printSchema()


    val df2 = df.select("studentid","name","marks","dept","subject").where("marks > 40")


    df2.writeStream
      .format("console")
      .format("csv")
      .option("path","C:/Users/yogesh.ahire/Desktop/Study/dataWrite")
      .option("checkpointLocation", "C:/Users/yogesh.ahire/Desktop/Study/dataWrite/checkpoint")
      .start()
      .awaitTermination()

  }
}
