package synthesis.training.glue.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import synthesis.training.glue.spark.DataTransfer.{writeToMySQL, writeToS3}
import synthesis.training.glue.spark.DataTypes.{RawData, casualtyPerFactor, collisionTypeCountPerYear, injuriesPerFactor, injuriesPerFactorPerYear}

object JobRunner {
  //Create a Spark session
  implicit val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("Glue_Crash_Analysis")
    .getOrCreate()
  def main(args: Array[String]): Unit = {
    val dataSource:String = args(0).toLowerCase()
    val dataSink:String = args(1).toLowerCase()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //Read data from Source
    val rawData: Dataset[RawData] = dataSource match {
      case "s3" => DataTransfer.readFromS3()
      case "mysql" => DataTransfer.readFromMySQL()
      case _ => throw sys.error("Source not found/implemented yet")
    }

    //Process the data
    val processedData:(Dataset[collisionTypeCountPerYear], Dataset[injuriesPerFactorPerYear],
      Dataset[injuriesPerFactor], Dataset[casualtyPerFactor]) = DataProcessor.processData(rawData)

    //Write processed data to Sink
    dataSink match {
      case "s3" =>
        writeToS3(processedData._1, "collisionTypeCountPerYear/")
        writeToS3(processedData._2, "injuriesPerFactorPerYear/")
        writeToS3(processedData._3,"injuriesPerFactor/")
        writeToS3(processedData._4,"casualtyPerFactor/")
      case "mysql" =>
        writeToMySQL(processedData._1, "collisionTypeCountPerYear")
        writeToMySQL(processedData._2, "injuriesPerFactorPerYear")
        writeToMySQL(processedData._3, "injuriesPerFactor")
        writeToMySQL(processedData._4, "casualtyPerFactor")
      case _ =>
        println("Error processing data: " + processedData._1.show(3))
        throw sys.error("Sink not found/implemented yet")
    }

    //Stop the Spark session
    println("Job complete, stopping Spark")
    spark.stop()
  }
}