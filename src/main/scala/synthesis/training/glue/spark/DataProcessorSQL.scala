package synthesis.training.glue.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset
import synthesis.training.glue.spark.DataTypes.{casualtyPerFactor, collisionTypeCountPerYear, injuriesPerFactor, injuriesPerFactorPerYear}
import synthesis.training.glue.spark.JobRunner.spark

object DataProcessorSQL {

  import spark.implicits._

  def processData(rawCrashData: Dataset[DataTypes.RawData]): (Dataset[DataTypes.collisionTypeCountPerYear],
    Dataset[DataTypes.injuriesPerFactorPerYear], Dataset[DataTypes.injuriesPerFactor], Dataset[DataTypes.casualtyPerFactor]) = {

    //Calculating number of collisions according to their type per year
    val collisionTypeCountPerYear: Dataset[collisionTypeCountPerYear] = spark.sql(
      "   SELECT year, collision_type, COUNT(*) AS count         " +
              "   FROM rawCrashData                                      " +
              "   GROUP BY year, collision_type;                         "
    ).as[collisionTypeCountPerYear]


    //Calculating number of injuries per factor per year
    val injuriesPerFactorPerYear: Dataset[injuriesPerFactorPerYear] = spark.sql(
      "   SELECT year, primary_factor, COUNT(*) AS count           " +
               "   FROM rawCrashData                                        " +
               "   GROUP BY year, primary_factor;                           "
      ).as[injuriesPerFactorPerYear]

    //Calculating number of injuries per factor per year
    val injuriesPerFactor: Dataset[injuriesPerFactor] = spark.sql(
      "   SELECT primary_factor, COUNT(*) AS count                  " +
        "         FROM rawCrashData                                          " +
        "         GROUP BY primary_factor                                    " +
        "         ORDER BY count;                                            "
    )
      .as[injuriesPerFactor]

    //Calculating which factor is most incapacitating
    val casualtyPerFactor: Dataset[casualtyPerFactor] = spark.sql(
      "   SELECT primary_factor, COUNT(*) AS count                  " +
        "          FROM rawCrashData                                        " +
        "          WHERE injury_type = 'Incapacitating'                     " +
        "          GROUP BY primary_factor                                  " +
        "          ORDER BY count;                                          "
    )
      .as[casualtyPerFactor]

    println(" Data Processing complete")
    (collisionTypeCountPerYear, injuriesPerFactorPerYear, injuriesPerFactor, casualtyPerFactor)
  }
}
