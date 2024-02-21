import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import synthesis.training.glue.spark.DataProcessor
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import synthesis.training.glue.spark.DataTypes.{RawData, casualtyPerFactor, collisionTypeCountPerYear, injuriesPerFactor, injuriesPerFactorPerYear}
import synthesis.training.glue.spark.JobProperties.s3SampleOutputUri

class DataProcessorTest extends AnyFunSuite with BeforeAndAfter {
  val sparkSession:SparkSession = SparkSession.builder
    .appName("DataProcessorTest")
    .master("local[2]")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")

  after {
    // Stop the SparkSession after each test
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }
  test("dataProcessor"){
    import sparkSession.implicits._
    //read sample data
    val rawDataSchema = Encoders.product[RawData].schema
    val rawSampleData:Dataset[RawData] = sparkSession.read.option("header","true").schema(rawDataSchema).csv(s3SampleOutputUri).as[RawData]

    val processedData: (Dataset[collisionTypeCountPerYear], Dataset[injuriesPerFactorPerYear],
      Dataset[injuriesPerFactor], Dataset[casualtyPerFactor]) = DataProcessor.processData(rawSampleData)

    // Perform assertions on the result DataFrames
    assertCollisionTypeCountPerYearEquals(processedData._1)
    assertInjuriesPerFactorPerYearEquals(processedData._2)
    assertInjuriesPerFactorEquals(processedData._3)
    assertCasualtyPerFactorEquals(processedData._4)
    System.out.println(" Tests passed!")
  }

  private def assertCollisionTypeCountPerYearEquals(actual: Dataset[collisionTypeCountPerYear]): Unit = {
    import sparkSession.implicits._
    val expected = Seq(
      (2011, "2-Car", 2),
      (2011, "1-Car", 1),
      (2006, "2-Car", 1),
      (2006, "1-Car", 1)
    ).toDF("year", "collision_type", "count")
      .as[collisionTypeCountPerYear].orderBy($"count".desc)
    assertResult(expected.collect())(actual.orderBy($"count".desc).collect())
  }

  private def assertInjuriesPerFactorPerYearEquals(actual: Dataset[injuriesPerFactorPerYear]): Unit = {
    import sparkSession.implicits._
    val expected = Seq(
      (2011, "SPEED TOO FAST FOR WEATHER CONDITIONS", 2),
      (2006, "RAN OFF ROAD RIGHT", 1),
      (2006, "SPEED TOO FAST FOR WEATHER CONDITIONS", 1),
      (2011, "UNSAFE BACKING", 1)
    ).toDF("year","primary_factor","count").as[injuriesPerFactorPerYear]
    assertResult(expected.orderBy($"count".desc).collect())(actual.orderBy($"count".desc).collect())
  }

  private def assertInjuriesPerFactorEquals(actual: Dataset[injuriesPerFactor]): Unit = {
    import sparkSession.implicits._
    val expected = Seq(
      ("SPEED TOO FAST FOR WEATHER CONDITIONS", 3),
      ("RAN OFF ROAD RIGHT", 1),
      ("UNSAFE BACKING", 1)
    ).toDF("primary_factor", "count").as[injuriesPerFactor]
    assertResult(expected.orderBy($"count".desc).collect())(actual.orderBy($"count".desc).collect())
  }

  private def assertCasualtyPerFactorEquals(actual: Dataset[casualtyPerFactor]): Unit = {
    import sparkSession.implicits._
    val expected = Seq(
      ("SPEED TOO FAST FOR WEATHER CONDITIONS", 1)
    ).toDF("primary_factor","count").as[casualtyPerFactor]
    assertResult(expected.orderBy($"count".desc).collect())(actual.orderBy($"count".desc).collect())
  }
}