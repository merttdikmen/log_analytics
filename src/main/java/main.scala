import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, isnull, regexp_replace, split}
import org.apache.spark.sql.types.{StringType, StructType}

object main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Long Analytics")
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
    val most_request=data
      .select("USER AGENT")
      .map(row => if (row.getString(0).split(" ").length == 0) "-"
      else row.getString(0).split(" ")(0).split("/")(0))
      .groupBy("value")
      .count()
      .orderBy(col("count").desc)

    // Hourly request rate
    val hourly_requested_rate=data
      .select("TIMESTAMP")
      .map(row => row.getString(0).split(":")(1)) // hours
      .groupBy("value")
      .count()
      .withColumnRenamed("value", "hour_of_day")
      .withColumnRenamed("count", "num_request")
      .orderBy(col("hour_of_day").asc)

    // Most popular content
    val most_popular_content=data
      .select("PATH")
      .map(row => if (row.getString(0).split(" ").length < 2) row.getString(0)
      else row.getString(0).split(" ")(1))
      .filter(path => path.compare("-") != 0)
      .groupBy("value")
      .count()
      .withColumnRenamed("value", "content")
      .orderBy(col("count").desc)

    val rates_to_date_country = data.select("PATH", "TIMESTAMP").filter(x => x.get(0).toString.length > 2 && x.get(0).toString.contains(" /"))
      .map(value => (value.get(0).toString.substring(0, value.mkString.indexOf(" /")), value.get(1).toString.substring(4, 12)))
      .groupBy("_1", "_2").count()

    //Sunucu kaynaklı erişilemeyen web sitesi hatası oranı
    val values = data.groupBy("STATUS CODE").count()
    val not_found = data.filter(col("STATUS CODE") === "404").select("URI").distinct

    //Şifrelenmiş URL’lerin decoder ile çözülmüş hali
    import java.nio.charset.StandardCharsets
    val coded=data.select("PATH").filter(x => x.toString.length > 2 && x.get(0).toString.contains(" /"))
      .map(value => value.toString.substring(value.mkString.indexOf(" /") + 3))
      .filter(line => line.mkString.contains("%"))
    val decoded_url=coded.map(a=>java.net.URLDecoder.decode(a.mkString, StandardCharsets.UTF_8))
    // result.distinct().show(false)

    //Log dosyalarındaki IP bilgisinden yararlanılarak ülke bazında siteye yapan kullanıcıların en çok tercih edilen arama motorlarının oranı
    val ipLoc = spark.read.format("csv").option("header", "false").load("src/resources/ip2loc.csv")
    val joinTable = data.withColumn("joinIP", regexp_replace(col("IP"), "\\.", ""))
    val location = ipLoc.join(joinTable, ipLoc("_c1") >= joinTable("joinIP") && ipLoc("_c0") <= joinTable("joinIP"), "right")
    val loc_count = location.groupBy(col("_c2")).count().withColumnRenamed("_c2", "Country")

    //Salamender üzerinden erişim yapanların bot olma yüzdesi, botların çoğunlukla hangi browser üzerinden geldiği
   /* val how_google_bot = data.select(col("USER AGENT")).filter(line => line.mkString.contains("bot.html")).count()
    val how_bing_bot = data.select(col("USER AGENT")).filter(line => line.mkString.contains("bingbot.htm")).count()
    val which_bot = (how_google_bot, how_bing_bot)*/

    /*
    write2db(most_request, "most_request")
    write2db(hourly_requested_rate, "hourly_requested_rate")
    write2db(most_popular_content, "most_popular_content")
    write2db(rates_to_date_country, "rates_to_date_country")
    write2db(not_found, "not_found")
    write2db(loc_count, "loc_count")
     */

  }

  def write2db(dataFrame: DataFrame, tableName: String): Unit = {
    dataFrame.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/log_analytics")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", s"$tableName")
      .option("user", "root")
      .option("password", "my-secret-pw")
      .mode("overwrite")
      .saveAsTable(s"$tableName")
  }

}




