package demo

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object TestTransformation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]")
                .appName("sparkTransformation")
                .config("spark.sql.shuffle.partition",2)
                .getOrCreate()

    val schema = new StructType()
      .add("studentid",IntegerType)
      .add("name",StringType)
      .add("department",StringType)
      .add("subject",StringType)
      .add("marks",DoubleType)


    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.readStream
            .format("csv")
            .option("header","true")
            .option("maxFilesPerTrigger","1")
            .schema(schema)
            .load("C:\\yo\\data\\data-archival-strategy")

    // spark SQL
    // df1.createTempView("student")
    // val resultDf = spark.sql("select *  from student where studentid > '300' ")

    // -- filter operation
   val resultDf = df1.filter("studentid > 200")
      .groupBy("department")
      .agg(functions.sum("marks"))

    resultDf.writeStream
      .format("console")
      .outputMode("complete")
      //.outputMode("append")
      .start()
      .awaitTermination()

  }

}
