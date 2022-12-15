import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{StringType, StructType}

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Compute Daily Page Views")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.textFile("src/resources/401.txt")
    val splitted = df.map(value => (
      value.substring(0, value.indexOf(" - - ")),
      value.substring(value.indexOf(" - - ") + 5, value.indexOf("]") + 1),
      value.substring(value.indexOf("\"") + 1, value.indexOf("\"", value.indexOf("\"") + 1)),
      value.substring(value.indexOf("\"", value.indexOf("\"") + 1) + 1)))
      .withColumn("temp", split(col("_4"), "\"")).select(col("*") +: (0 until 4).map(i => col("temp").getItem(i).as(s"col$i")): _*)
      .drop("col2").drop("temp").drop("_4")
      .withColumn("temp", split(col("col0"), " ")).select(col("*") +: (0 until 3).map(i => col("temp").getItem(i).as(s"cols$i")): _*).drop("cols0").drop("temp").drop("col0")

    val temp = splitted.withColumnRenamed("_1", "IP").
      withColumnRenamed("_2", "TIMESTAMP").
      withColumnRenamed("_3", "PATH").
      withColumnRenamed("col1", "URI").
      withColumnRenamed("col3", "USER AGENT").
      withColumnRenamed("cols1", "STATUS CODE").
      withColumnRenamed("cols2", "SIZE")

    val data = temp.na.drop()

    // User agents that send the most requests
    data
      .select("USER AGENT")
      .map(row => if (row.getString(0).split(" ").length == 0) "-"
                  else row.getString(0).split(" ")(0).split("/")(0))
      .groupBy("value")
      .count()
      .orderBy(col("count").desc)
      .show(30, false)

    // Hourly request rate
    data
      .select("TIMESTAMP")
      .map(row => row.getString(0).split(":")(1))  // hours
      .groupBy("value")
      .count()
      .withColumnRenamed("value", "hour_of_day")
      .withColumnRenamed("count", "num_request")
      .orderBy(col("hour_of_day").asc)
      .show(24, false)

    // Most popular content
    data
      .select("PATH")
      .map(row => if (row.getString(0).split(" ").length < 2) row.getString(0)
                  else row.getString(0).split(" ")(1))
      .filter(path => path.compare("-") != 0)
      .groupBy("value")
      .count()
      .withColumnRenamed("value", "content")
      .orderBy(col("count").desc)
      .show(30, false)

  }
  def errorCode(data: DataFrame):DataFrame={
    val values = data.groupBy("STATUS CODE").count()
    val not_found = data.filter(col("STATUS CODE") === "404").select("URI").distinct
    not_found
  }
  def countryRate(df: DataFrame,spark:SparkSession):DataFrame={
    val ipLoc = spark.read.format("csv").option("header", "false").load("ip2loc.csv")
    val joinTable = df.withColumn("joinIP", regexp_replace(col("IP"), "\\.", ""))
  val location = ipLoc.join(joinTable, ipLoc("_c1") >= joinTable("joinIP") && ipLoc("_c0") <= joinTable("joinIP"), "right")
    location.groupBy("_c2").count()
    location
  }
  def reqTypes(df:DataFrame):DataFrame={
    df.select(col("PATH")).filter(z=>z.mkString.isEmpty()).count()
    //boş pathlilerin sayısı
    val nonEmptyReqs=df.select(col("PATH")).filter(x=>x.mkString.nonEmpty)
    nonEmptyReqs
  }
}
