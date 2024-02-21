package synthesis.training.glue.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset
import synthesis.training.glue.spark.DataTypes.{casualtyPerFactor, collisionTypeCountPerYear, injuriesPerFactor, injuriesPerFactorPerYear}
import synthesis.training.glue.spark.JobRunner.spark
/**
 * @author ${Minesh.Melvin}
 */
object DataProcessor {
    import spark.implicits._
    def processData(rawCrashData: Dataset[DataTypes.RawData]): (Dataset[DataTypes.collisionTypeCountPerYear],
      Dataset[DataTypes.injuriesPerFactorPerYear], Dataset[DataTypes.injuriesPerFactor], Dataset[DataTypes.casualtyPerFactor]) = {

        //Calculating number of collisions according to their type per year
        val collisionTypeCountPerYear:Dataset[collisionTypeCountPerYear] = rawCrashData
          .groupBy(col("year"), col("collision_type"))
          .count()
          .as[collisionTypeCountPerYear]

        //Calculating number of injuries per factor per year
        val injuriesPerFactorPerYear:Dataset[injuriesPerFactorPerYear] = rawCrashData
          .groupBy(col("year"), col("primary_factor"))
          .count()
          .as[injuriesPerFactorPerYear]

        //Calculating number of injuries per factor per year
        val injuriesPerFactor:Dataset[injuriesPerFactor] = rawCrashData
          .groupBy(col("primary_factor"))
          .count().orderBy(col("count"))
          .as[injuriesPerFactor]

        //Calculating which factor is most incapacitating
        val casualtyPerFactor:Dataset[casualtyPerFactor] = rawCrashData
          .where(col("injury_type") === "Incapacitating")
          .groupBy(col("primary_factor"))
          .count().orderBy(col("count"))
          .as[casualtyPerFactor]

        println(" Data Processing complete")
        (collisionTypeCountPerYear, injuriesPerFactorPerYear, injuriesPerFactor, casualtyPerFactor)
    }
}


/** RAW SAMPLE INPUT USED TO DEVELOP CODE
 * val rawCrashData: Dataset[RawData] = Seq(
 * RawData(2015, 1, 5, "Weekday", 1500, "2-Car", "No Injury/unknown", "ROADWAY SURFACE CONDITION", "LIBERTY", 39.19927, -86.637),
 * RawData(2015, 3, 15, "Weekday", 2100, "2-Car", "Incapacitating", "FOLLOWING TOO CLOSELY", "ROGERS & W PATTERSON", 39.19927, -86.637),
 * RawData(2016, 2, 13, "Weekend", 1600, "2-Car", "No Injury/unknown", "ROADWAY SURFACE CONDITION", "LIBERTY", 39.19927, -86.637),
 * RawData(2016, 2, 24, "Weekday", 1900, "1-Car", "No Injury/unknown", "FOLLOWING TOO CLOSELY", "ROGERS & W PATTERSON", 39.19927, -86.637),
 * RawData(2017, 1, 31, "Weekend", 1400, "1-Car", "Incapacitating", "FOLLOWING TOO CLOSELY", "LIBERTY", 39.19927, -86.637)
 * ).toDS().as[RawData]
 * rawCrashData.show()
 *
 * //.toDF("year", "month", "day", "weekend?", "hour", "collisionType", "injuryType", "primaryFactor", "reportedLocation", "latitude", "longitude")
 *
*/
