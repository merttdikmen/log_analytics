import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Compute Daily Page Views")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val dummy = spark.read.textFile("pom.xml")

    dummy.show()
  }
}
