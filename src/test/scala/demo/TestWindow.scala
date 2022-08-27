package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

import scala.concurrent.duration.DurationInt

object TestWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .appName("sparkTransformation")
      .config("spark.sql.shuffle.partition",2)
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("order_id",IntegerType)
      .add("order_date",TimestampType)
      .add("order_customer_id",IntegerType)
      .add("order_status",StringType)
      .add("amount",DoubleType)

    val orderDf = spark.readStream
      .format("csv")
      .option("header","true")
      .option("maxFilesPerTrigger","1")
      .schema(schema)
      .load("C:\\yo\\TestWindowData")

    val windowDf = orderDf.groupBy(window(col("order_date"),"15 minute"))
      .agg(sum("amount")).alias("totalInvoice")



    val resultDf = windowDf.select("window.start","window.end","sum(amount)")


    resultDf.writeStream
      .format("console")
      //.outputMode("complete")
      .outputMode("update")
        .trigger(Trigger.ProcessingTime(50.seconds))
      .start()
      .awaitTermination()
  }

}
