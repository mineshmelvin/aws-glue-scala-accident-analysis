package synthesis.training.glue.spark

object JobProperties {
  // Set your AWS credentials (make sure to store them more securely than this, Glue provides a functionality for that)
  val exampleAccessKeyId = "<exampleAccessKeyId>"
  val exampleSecretAccessKey = "<exampleSecretAccessKey>"

  // Set the S3 raw data URI
  val s3InputUri = s"""s3a://crash-data-demo-bucket/data/crash_data.csv"""
  val s3OutputUri = s"""s3a://crash-data-demo-bucket/output/"""
  // local location for storing extracted sample data from above source bucket for testing in local environment
  val s3SampleOutputUri = s"""file:\\C:\\Users\\mines\\workspace\\projects\\glue-demo\\glue_scala_demo\\src\\test\\resources\\sample_data"""

  // Data Properties
  val url = "jdbc:mysql://<username>@glue-rds-database-delete.something.us-east-1.rds.amazonaws.com:3306/glue_db?useSSL=false"
  val tableName = "crash_data"
  val user = "<username>"
  val password = "<password>"
  val dbtable = "glue_db.crash_data"
  val driver = "com.mysql.jdbc.Driver"
}
