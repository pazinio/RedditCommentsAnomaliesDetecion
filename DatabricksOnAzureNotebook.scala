// Databricks notebook on azure source

/** Adls configurator **/
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "************)
spark.conf.set("dfs.adls.oauth2.credential", "***************")
spark.conf.set("dfs.adls.oauth2.refresh.url", "************************")

/** Basic imports **/
import spark.implicits._
import org.apache.spark.sql.functions._
import java.util.Date
import java.time.ZoneId.systemDefault
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.Instant

/** Basic Configuration **/
val adls = "adl://****.azuredatalakestore.net"

/** udfs **/
val dayDate = udf((utc:Long) => new Date(utc * 1000l).toInstant.atZone(systemDefault).toLocalDate.toString)
val dayOfWeek = udf((utc:Long) => ZonedDateTime.ofInstant(Instant.ofEpochMilli(utc * 1000l), ZoneId.of("Etc/UTC")).getDayOfWeek.toString)
val toHour = udf((utc:Long) => ZonedDateTime.ofInstant(Instant.ofEpochMilli(utc * 1000l), ZoneId.of("Etc/UTC")).getHour.toString)


// COMMAND ----------

// read reddit raw comment as json format [~500G]
val raw = spark.read.json(adls + s"/RC_2017-*/*").filter($"created_utc".isNotNull)

// partition comments by day 
val ds = 
    raw.select("author", "subreddit", "body", "created_utc")
    .withColumn("short_date", dayDate($"created_utc"))
    .withColumn("day_date", dayDate($"created_utc"))
    .withColumn("day_of_week", dayOfWeek($"created_utc"))
    .withColumn("hour", toHour($"created_utc"))
  
ds.write.mode("overwrite").option("header","true").partitionBy("day_date").format("parquet").save(adls + s"/reddit-comments/parquet/")
display(ds.limit(5))


// COMMAND ----------

/** Read partitioned data  **/
val df = spark.read.option("header", "true").parquet(adls + s"/reddit-comments/parquet/*/*")
df.cache()
display(df.limit(10))

// COMMAND ----------

// Find anomalies days
val dateToDaysOfWeek = df.select("short_date", "day_of_week").distinct

// agg by date (each day)
val res1 = df.groupBy($"short_date").agg(count($"body").alias("total_in_day"))

// join to add day of week (Sunday, Monday, ...)
val agg1 = res1.join(dateToDaysOfWeek, "short_date").orderBy($"total_in_day".desc) 

agg1.cache()
display(agg1.limit(10))

// COMMAND ----------

// agg by day of week (Sunday, Monday, ...)
val agg2 = agg1.groupBy($"day_of_week").agg(
  avg($"total_in_day").alias("avg"),
  stddev($"total_in_day").as("deviation")
).orderBy($"avg".desc)
display(agg2)


// COMMAND ----------

case class SumAvgDeviation(avg: Double, deviation:Double)
case class DayDateTotalInDayOfWeek(short_date:String, total_in_day: Double, day_of_week: String)
case class Result(short_date:String, total_in_day: Double, day_of_week: String, avg: Double, deviation:Double)

val aggByDate = agg1.as[DayDateTotalInDayOfWeek]
aggByDate.cache()

// collect week day statistics table to memory
val rows = agg2.collect
val mapDays = rows.map(r => (r.get(0), SumAvgDeviation(r.getDouble(1), r.getDouble(2)))).toMap

// mark day as anomaly day if total_in_day higher than the average plus twice deviation
val anomaliesDays = aggByDate.filter(
                          /*much higher*/ d => d.total_in_day >= ( mapDays(d.day_of_week).avg + (mapDays(d.day_of_week).deviation * 2.0 )) ||
                          /*much lower*/  d.total_in_day <= ( mapDays(d.day_of_week).avg - (mapDays(d.day_of_week).deviation * 2.0 ))
                         )
                        .map(r => Result(r.short_date, r.total_in_day, r.day_of_week, mapDays(r.day_of_week).avg, mapDays(r.day_of_week).deviation))
                        

display(anomaliesDays.orderBy($"total_in_day".desc).limit(20))

// COMMAND ----------

display(anomaliesDays.orderBy($"total_in_day").limit(20))

// COMMAND ----------

// schema -> // author:string // subreddit:string // body:string // created_utc:long // short_date:string // day_of_week:string // hour:string
import org.apache.spark.sql.types._

val mySchema = StructType(List(
      StructField("author", StringType, nullable=false),
      StructField("subreddit", StringType, nullable=false),
      StructField("body", StringType, nullable=false),
      StructField("created_utc", LongType, nullable=false),
      StructField("short_date", StringType, nullable=false),
      StructField("day_of_week", StringType, nullable=false),
      StructField("hour", StringType)
))

val df = spark
          .readStream
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
          .schema(mySchema)
          .parquet(adls + s"/reddit-comments/parquet/*/*")

// val commonWordsBySubreddit = df.groupBy("subreddit").agg(concat_ws(" ",collect_list("body")) as "words")

display(df)
