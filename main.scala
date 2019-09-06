import scala.io.Source
import java.net.{URL, URLDecoder}
import java.nio.charset.StandardCharsets

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, date_format, date_trunc, hour, lead, lit, udf, unix_timestamp, when, sum => summa}

object WebtrendsProcessing extends App {
  def getDictFromFile(filePath: String): Map[String, String] = {
    val dict = Source.fromFile(filePath, "utf-8").getLines().map(line => {
      val spellings = line.trim().split("=")
      spellings(0) -> spellings(1)
    }).toMap

    dict
  }

  def mapCity(city: String, state: String, geoMappings: Map[String, String]): String = {
    geoMappings.get(city).orElse(geoMappings.get(state)).getOrElse(city)
  }

  def mapState(state: String, geoMappings: Map[String, String]): String = {
    geoMappings.getOrElse(state, state)
  }

  val extractPath: (String, String) => String = (host, url) => {
    if (url.contains(host))
      new URL(url).getPath
    else
      "-"
  }

  val unquote = (str: String) => {
    val unescapedJava = StringEscapeUtils.unescapeJava(str.replace("%u", "\\u"))
    URLDecoder.decode(unescapedJava, StandardCharsets.UTF_8.name())
  }

  def getDfFromFile(path: String, spark: SparkSession) = {
    spark
      .read
      .format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(path)
  }

  def processFile(path: String, spark: SparkSession): Map[String, Double] = {
    val readFileStart = System.nanoTime()
    val df: DataFrame = getDfFromFile(path, spark)
    val readFileEnd = System.nanoTime()
    val readFileTime = (readFileEnd - readFileStart) / Math.pow(10, 9)

    val processingStart = System.nanoTime()
    // webtrends fields
    val webtrendsDateField = "date"
    val sessionIdField = "sessionid"
    val dataCsRefererField = "data_cs_referer"
    val sequenceNumberField = "sequence_number"

    // intermediate fields for calculations
    val nextTimeField = "nextTime"
    val timeOnPageField = "timeOnPage"
    val bounceField = "bounce"
    val entryField = "entry"
    val sessionPagesCountField = "sessionPageCount"

    // aggs/metrics fields
    val sessionsAggField = "sessions"
    val pageViewsAggField = "page views"
    val uniquePageViewsField = "unique page views"
    val bouncesAggField = "bounces"
    val timeOnPageAggField = "time on page"

    // dimension fields
    val hostnameField = "host name"
    val sourceField = "source"
    val mediumField = "medium"
    val countryField = "country"
    val pagePathField = "page path"
    val previousPagePathField = "previous page path"
    val dateField = "date"
    val hourField = "hour"
    val pageTitleField = "page title"
    val cityField = "city"
    val stateField = "state"

    // do mappings
    val dfMapped = df
      .withColumnRenamed("data_cs_host", hostnameField)
      .withColumnRenamed("ext_source_name", sourceField)
      .withColumnRenamed("ext_source_type", mediumField)
      .withColumnRenamed("ext_geo_country", countryField)
      .withColumnRenamed("data_cs_host", hostnameField)
      .withColumnRenamed("data_cs_uri_stem", pagePathField)
      .withColumnRenamed("data_wt_ti", pageTitleField)
      .withColumnRenamed("ext_geo_city", cityField)
      .withColumnRenamed("ext_geo_region", stateField)

    // split referrer url
    val dfRefUrl = dfMapped.withColumn(previousPagePathField, getPathUdf(col(hostnameField), col(dataCsRefererField)))

    // map country, city and state names
    val dfGeoMapped = dfRefUrl
      .withColumn(countryField, mapCountryUdf(col(countryField)))
      .withColumn(cityField, mapCityUdf(col(cityField), col(stateField)))
      .withColumn(stateField, mapStateUdf(col(stateField)))

    // calculate delta time between events
    val window = Window.partitionBy(sessionIdField).orderBy(webtrendsDateField)
    val dfTimeDelta = dfGeoMapped
      .withColumn(nextTimeField, lead(webtrendsDateField, 1, null).over(window))
      .withColumn(timeOnPageField, unix_timestamp(col(nextTimeField)) - unix_timestamp(col(webtrendsDateField)) )

    // count pages per session to get bounces
    val sessionPagesCounts = dfTimeDelta
      .groupBy(sessionIdField).count()
      .withColumnRenamed("count", sessionPagesCountField)
    val dfBounce = dfTimeDelta
      .join(sessionPagesCounts, List(sessionIdField))
      .withColumn(bounceField, when(col(sessionPagesCountField).equalTo(1),1).otherwise(0))

    // use sequence number to get entry pages
    val dfEntries = dfBounce.withColumn(entryField, when(col(sequenceNumberField).equalTo(0),1).otherwise(0))

    // convert to dateHour, truncating
    val dfHour = dfEntries
      .withColumn(hourField, hour(col(webtrendsDateField)))
      .withColumn(dateField, date_trunc("day", col(webtrendsDateField))) // truncate and date gets type timestamp
      .withColumn(dateField, date_format(col(dateField), "yyyy-MM-dd")) // convert date to string to get desired format

    // calculate aggregated metrics per hour
    val dfGrouped = dfHour.groupBy(
    dateField,
    hourField,
    hostnameField,
    sourceField,
    mediumField,
    countryField,
    stateField,
    cityField,
    pagePathField,
    previousPagePathField,
    pageTitleField)
      .agg(
    count(lit(1)).alias(pageViewsAggField), // page views / events count
    countDistinct(sessionIdField).alias(uniquePageViewsField), // unique page count
    summa(bounceField).alias(bouncesAggField), // bounces count
    summa(entryField).alias(sessionsAggField), // entries/sessions count
    avg(col(timeOnPageField)).alias(timeOnPageAggField) // time on page average
    )

    // fill empty Page Titles, and apply url decoding
    val dfNaFilledDecoded = dfGrouped
      .na.fill(Map(pageTitleField -> "Empty Title")).withColumn(pageTitleField, urlDecodeUdf(col(pageTitleField)))
      .na.fill(Map(timeOnPageAggField -> 0)) // fill in with zeros for null values of time on page

    dfNaFilledDecoded.filter("country = \"ACountry\"").filter("state = \"AState\"").show()

    val processingEnd = System.nanoTime()
    val processingTime = (processingEnd - processingStart) / Math.pow(10, 9)

    Map("processing" -> processingTime, "readFile" -> readFileTime)
  }

  val getPathUdf = udf(extractPath)
  val urlDecodeUdf = udf(unquote)

  val countryMappings = getDictFromFile("mapping-files/country-mappings.txt")
  val geoMappings = getDictFromFile("mapping-files/alt2GaCities.txt")

  val mapCountryUdf = udf((country: String) => countryMappings.getOrElse(country, country))
  val mapCityUdf = udf((city: String, state: String) => mapCity(city, state, geoMappings))
  val mapStateUdf = udf((state: String) => mapState(state, geoMappings))

  val master = "local"
  val appName = "webtrends-processing"

  val spark = SparkSession.builder()
    .master(master)
    .appName(appName)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val times = processFile("resources/bigdatafile.csv", spark)
  println("read file time: " + times("readFile"))
  println("processing time: " + times("processing"))
}
