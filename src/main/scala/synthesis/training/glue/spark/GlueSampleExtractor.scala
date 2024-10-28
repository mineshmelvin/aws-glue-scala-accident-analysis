package synthesis.training.glue.spark

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import synthesis.training.glue.spark.DataTypes.RawData
import synthesis.training.glue.spark.JobProperties.{s3InputUri, s3SampleOutputUri}

/**
 * Notes:
 * This object can be used to extract sample data from the source file for testing program in development environment
 * Add <property name="dynamic.classpath" value="true"/> to <component name="PropertiesComponent"> in .idea/workspace.xml
 */

object GlueSampleExtractor {

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("GlueSampleExtractor")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //Extract the schema of RawData to use while reading csv
    val schema = Encoders.product[RawData].schema

    try {
      import spark.implicits._
      // Read data from S3
      val dataSource:Dataset[RawData] = spark.read.schema(schema).csv(s3InputUri).as[RawData]
      dataSource.show(5)

      // Extract a sample of data
      val sampleData = dataSource.sample(0.000093) // Sample 5 lines of the data

      // Write sample data to local file
      sampleData.coalesce(1).write.option("header", "true").csv(s3SampleOutputUri)

      println(s"Sample data written to: $s3SampleOutputUri")
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}