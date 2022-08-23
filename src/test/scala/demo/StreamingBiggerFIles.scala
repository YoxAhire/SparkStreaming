package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import scala.concurrent.duration._

object StreamingBiggerFIles {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .getOrCreate()


    val schema = new StructType()
      .add("id",IntegerType)
      .add("amount",IntegerType)
      .add("location",StringType)
      .add("FirstName",StringType)
      .add("MiddleInitial",StringType)
      .add("LastName",StringType)
      .add("Gender",StringType)
      .add("EMail",StringType)
      .add("FathersName",StringType)
      .add("MothersName",StringType)
      .add("DateofBirth",StringType)
      .add("AgeinYrs",StringType)
      .add("WeightinKgs",StringType)
      .add("YearofJoining",StringType)
      .add("MonthofJoining",StringType)
      .add("Salary",StringType)
      .add("SSN",StringType)
      .add("PhoneNo",StringType)
      .add("PlaceName",StringType)
      .add("County",StringType)
      .add("City",StringType)
      .add("State",StringType)


    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("csv")
      .option("inferSchema","true")
      .option("header","true")
      .schema(schema)
      .load("C:/yo/data2");

    df.printSchema()


    val query = df.writeStream
                  .format("console")
                  //.outputMode("Append")
                  .format("parquet")
                  .trigger(Trigger.ProcessingTime(25.seconds))
                  .option("path","C:/Users/yogesh.ahire/Desktop/Study/dataWrite1/destination1")
                  .option("checkpointLocation", "C:/Users/yogesh.ahire/Desktop/Study/dataWrite1/checkpointing1")
                  .start()

    query.awaitTermination()

  }
}
