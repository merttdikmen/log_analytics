import org.apache.spark.sql.SparkSession
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

    //Sayfaya gelen istek tiplerinin aylık analizi
    val rates_to_date_country = data.select("PATH", "TIMESTAMP").filter(x => x.get(0).toString.length > 2 && x.get(0).toString.contains(" /"))
    .map(value => (value.get(0).toString.substring(0, value.mkString.indexOf(" /")), value.get(1).toString.substring(4, 12)))
    .groupBy("_1", "_2").count()

   //Sunucu kaynaklı erişilemeyen web sitesi hatası oranı
    val values = data.groupBy("STATUS CODE").count()
    val not_found = data.filter(col("STATUS CODE") === "404").select("URI").distinct


    //Log dosyalarındaki IP bilgisinden yararlanılarak ülke bazında siteye yapan kullanıcıların en çok tercih edilen arama motorlarının oranı
    val ipLoc = spark.read.format("csv").option("header", "false").load("src/resources/ip2loc.csv")
    val joinTable = df.withColumn("joinIP", regexp_replace(col("IP"), "\\.", ""))
    val location = ipLoc.join(joinTable, ipLoc("_c1") >= joinTable("joinIP") && ipLoc("_c0") <= joinTable("joinIP"), "right")
    val loc_count=location.groupBy("_c2").count()


    val how_google_bot = data.select(col("USER AGENT")).filter(line => line.mkString.contains("bot.html")).count()
    val how_bing_bot = data.select(col("USER AGENT")).filter(line => line.mkString.contains("bingbot.htm")).count()
    val which_bot=  (how_google_bot, how_bing_bot)

  }
}
