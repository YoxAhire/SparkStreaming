package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object TestJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .appName("sparkTransformation")
      .config("spark.sql.shuffle.partition",2)
      .getOrCreate()

    val orgSchema = new StructType()
      .add("orgid",IntegerType)
      .add("orgname",StringType)
      .add("domain",StringType)
      .add("service",StringType)
      .add("headquarter",StringType)

    val empSchema = new StructType()
      .add("empOrgId",IntegerType)
      .add("id",IntegerType)
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

    val df1 = spark.readStream
      .format("csv")
      .option("header","true")
      .schema(empSchema)
      .load("C:\\yo\\Transformation\\Employee")

    val df2 = spark.readStream
      .format("csv")
      .option("header","true")
      .schema(orgSchema)
      .load("C:\\yo\\Transformation\\Organization")

    //df1.printSchema()
    //df2.printSchema()

    val resultDf = df2.join(df1,expr("empOrgId == orgid and Salary > 25000"))

    /*df2.writeStream
      .format("console")
      .start()
      //.awaitTermination()


    df1.writeStream
      .format("console")
      .start()
      .awaitTermination()*/

    resultDf.writeStream
      .format("console")
      .start()
      .awaitTermination()

  }
}
