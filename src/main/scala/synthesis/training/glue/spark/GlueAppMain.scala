package synthesis.training.glue.spark
object GlueAppMain {
  def main(sysArgs: Array[String]): Unit = {
    val args = Array("mysql", "mysql") // Can be issued with the Glue UI
    JobRunner.main(args)
  }
}