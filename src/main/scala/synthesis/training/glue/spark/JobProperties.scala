package synthesis.training.glue.spark

object JobProperties {
  // Set your AWS credentials (make sure to replace these with your actual credentials)
  val accessKeyId = "AKIAUJ62OUGNEC2GMHEF"
  val secretAccessKey = "6JXNOx7gb4v+6yy/QIQOFR3VUt7FRydujJr8ADzs"

  // Set the S3 raw data URI
  val s3InputUri = s"""s3a://minesh-glue-demo-delete/data/crash_data.csv"""
  val s3OutputUri = s"""s3a://minesh-glue-demo-delete/output/"""
  val s3SampleOutputUri = s"""file:\\C:\\Users\\mines\\workspace\\projects\\glue-demo\\glue_scala_demo\\src\\test\\resources\\sample_data"""

  // Data Properties
  val url = "jdbc:mysql://admin@minesh-glue-rds-database-delete.cswckcclgwnu.us-east-1.rds.amazonaws.com:3306/glue_db?useSSL=false"
  val tableName = "crash_data"
  val user = "admin"
  val password = "password"
  val dbtable = "glue_db.crash_data"
  val driver = "com.mysql.jdbc.Driver"
}
