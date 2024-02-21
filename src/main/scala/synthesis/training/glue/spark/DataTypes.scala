package synthesis.training.glue.spark

object DataTypes {
  case class RawData(
                      year: Int,
                      month: Int,
                      day: Int,
                      weekend: String,
                      hour: Int,
                      collision_type: String,
                      injury_type: String,
                      primary_factor: String,
                      reported_location: String,
                      latitude: Double,
                      longitude: Double
                    )

  case class collisionTypeCountPerYear(
                                      year: Int,
                                      collision_type: String,
                                      count: BigInt
                                      )

  case class injuriesPerFactorPerYear(
                                     year: Int,
                                     primary_factor: String,
                                     count: BigInt
                                     )
  case class injuriesPerFactor(
                              primary_factor: String,
                              count: BigInt
                              )
  case class casualtyPerFactor(
                                primary_factor: String,
                                count: BigInt
                              )
}
