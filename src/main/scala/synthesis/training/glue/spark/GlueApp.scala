package synthesis.training.glue.spark
object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val args = Array("mysql", "mysql")
    JobRunner.main(args)
  }
}

/**
 * val spark: SparkSession = SparkSession.builder
 * .appName("Glue_Crash_Analysis")
 * .getOrCreate()
 * import spark.implicits._
 *
 * val df:DataFrame = spark.read.format("jdbc")
 * .option("url", "jdbc:mysql://minesh-glue-rds-database-delete.cswckcclgwnu.us-east-1.rds.amazonaws.com:3306/glue_db?useSSL=false")
 * .option("driver", "com.mysql.jdbc.Driver")
 * .option("dbtable", "crash_data")
 * .option("user", "admin")
 * .option("password", "password")
 * .load()
 * println(df.show())
 */
