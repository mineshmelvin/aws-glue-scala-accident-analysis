package synthesis.training.glue.spark

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import synthesis.training.glue.spark.DataTypes.RawData
import synthesis.training.glue.spark.JobProperties.{driver, password, s3InputUri, s3OutputUri, tableName, url, user}
object DataTransfer {
  /**
   * Method that reads from an S3 source
   * @param spark, the SparkSession to use
   * @return RawData from S3 bucket
   */
  def readFromS3(spark:SparkSession): Dataset[RawData] = {
    import spark.implicits._
    // Read the file from S3 into a DataFrame
    val ds = spark.read.schema(Encoders.product[RawData].schema).csv(s3InputUri).as[RawData]
    ds
  }

  /**
   * Method that reads from a MySQL source
   *
   * @param spark , the SparkSession to use
   * @return RawData from S3 bucket
   */
  def readFromMySQL(spark:SparkSession):Dataset[RawData] = {
    import spark.implicits._
    val ds:Dataset[RawData] = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .schema(Encoders.product[RawData].schema)
      .load().as[RawData]
    ds
  }

  /**
   * Method that writes a dataset to S3
   *
   * @param ds Dataset to write
   */
  def writeToS3[T](ds: Dataset[T], path:String):Unit = {
    ds.coalesce(1).write.csv(s3OutputUri+path)
  }

  /**
   * Method that writes a dataset to MySQL
   *
   * @param ds Dataset to write
   */
  def writeToMySQL[T](ds: Dataset[T], tableName: String): Unit = {
    ds.coalesce(1).write
      .format("jdbc")
      .mode("overwrite")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .save()
    println("Dataset written to table: "+tableName+".")
  }
}