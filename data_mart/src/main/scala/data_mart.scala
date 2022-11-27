import model.schemas.schemaWeblogs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import util.udfUtil.{getCategoryAge, getHostDecodeUrl}

import java.sql.{Connection, DriverManager}
import scala.util.Properties

object data_mart extends App {

  val spark: SparkSession = SparkSession
    .builder
    .appName("data_mart_for_exemple")
    .getOrCreate()

  Logger.getRootLogger.setLevel(Level.ERROR)
  
  // HFDS
  val sourceJson =  Properties.envOrElse("SOURCE_HDFS_JSON","undefined");
  // Elasticsearch
  val usernameEs = Properties.envOrElse("USER_ES","undefined");
  val hostEs = Properties.envOrElse("HOST_ES","undefined");
  val portEs = Properties.envOrElse("PORT_ES","undefined");
  val passwordEs = Properties.envOrElse("PASS_ES","undefined");
  // Cassandra
  val usernameCs = Properties.envOrElse("USER_CS","undefined");
  val hostCs = Properties.envOrElse("HOST_CS","undefined");
  val passwordCs = Properties.envOrElse("PASS_CS","undefined");
  val sourceTableCs = "clients";
  val sourceKeyspaceCs = "labdata";
  // PostgreSQL
  val usernamePg = Properties.envOrElse("USER_PG","undefined");
  val hostPg = Properties.envOrElse("HOST_PG","undefined");
  val passwordPg = Properties.envOrElse("PASS_PG","undefined");
  val schemaPg = "labdata";
  val sourceTablePg = "domain_cats";
  // Target
  val targetTable = "clients";
  // Register udf
  val getCategoryAgeUDF: UserDefinedFunction = udf(getCategoryAge)
  val getHostDecodeUrlUDF: UserDefinedFunction = udf(getHostDecodeUrl)

  /* a. Client information
  * Customer information is stored in Cassandra(keyspace labdata, table clients):
  * uid – unique user ID, string
  * gender – user gender, F or M - string
  * age – user age in years, integer
  */

  spark.conf.set("spark.cassandra.connection.host", s"${hostCs}")

  val clientsDF: Dataset[Row] =
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("table", s"${sourceTableCs}")
      .option("keyspace", s"${sourceKeyspaceCs}")
      .load()
      .repartition(8)

  val clientsCleanDF: Dataset[Row] = clientsDF
    .withColumn("age_cat", getCategoryAgeUDF(col("age")))
    .select(col("uid"), col("gender"), col("age_cat"))

  /*b. Online store visit logs
  * Elasticsearch index: visits
  *
  * Filtered and enriched messages about product page views and purchases come from the backend of the online store.
  * Messages are stored in Elasticsearch in json format in the following form:
  *
  * uid – unique user ID (the same as in Cassandra) or null if there is no information about these visitors, string
  * event_type – buy or view, respectively purchase or view of goods, string
  * category – category of goods in the store, string
  * item_id – product ID, that consists of the category and the number of the product, string
  * item_price – price, integer
  * timestamp – unix epoch timestamp in ms
  */

  val visitsDF: Dataset[Row] =
    spark
      .read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", s"${hostEs}")
      .option("es.port", s"${portEs}")
      .option("es.net.http.auth.user", s"${usernameEs}")
      .option("es.net.http.auth.pass", s"${passwordEs}")
      .option("es.nodes.wan.only", "true")
      //.option("es.mapping.date.rich", "false")
      .load("visits")
      .repartition(8)

  val visitsCleanDF: Dataset[Row] = visitsDF
    .select(col("uid").as("uidv"), col("category"))
    .withColumn("category", regexp_replace(col("category"), " ", "_"))
    .withColumn("category", regexp_replace(col("category"), "-", "_"))
    .withColumn("category", lower(concat(lit("shop_"), col("category"))))

  /*c. Website visit logs 
  * hdfs: weblogs.json
  *
  * Logs are stored in json on HDFS and have that following structure:
  *
  * uid – unique user ID (the same as in Cassandra),
  * array visits with some number of pairs (timestamp, url), where timestamp – unix epoch timestamp in ms, url - string.
  * In this datasets not each record has uid.
  * It means than there were customers who has not been indendified and added in the DB
  * Purchases and views of such customers can be ignored.
  */

  val weblogsDF: Dataset[Row] = spark
    .read
    .format("json")
    .schema(schemaWeblogs)
    .load(s"${sourceJson}")
    .repartition(8)

  val weblogsCleanDF: Dataset[Row] = weblogsDF
    .withColumn("new_visits", explode(col("visits")))
    .select(col("uid"), col("new_visits.timestamp").as("ts"), col("new_visits.url").as("urls"))
    .where(col("urls").contains("http") or col("urls").contains("https"))
    .withColumn("url_dec", getHostDecodeUrlUDF(col("urls")))
    .withColumn("url_clean", trim(col("url_dec")))
    .withColumn("url_clean", regexp_replace(col("url_clean"), "^www\\.", ""))
    .select(col("uid").as("uidw"), col("url_clean").as("url"))

  /*d. Website Category Information
  * This information is stored in PostgreSQL labdata in table domain_cats:
  * domain (only second level), string
  * category, string
  */

  val domainCatsDF: Dataset[Row] =
    spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://${hostPg}/${schemaPg}?user=${usernamePg}&password=${passwordPg}")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", s"${sourceTablePg}")
      .load()
      .repartition(8)

// Preparation and calculation
  val domainCatsCleanDF: Dataset[Row] = domainCatsDF
    .withColumn("category", regexp_replace(col("category"), " ", "_"))
    .withColumn("category", regexp_replace(col("category"), "-", "_"))
    .withColumn("category", lower(concat(lit("web_"), col("category"))))
    .select(col("domain"), col("category"))

  val visitsAggDF: Dataset[Row] = visitsCleanDF
    .groupBy(col("uidv"))
    .pivot(col("category"))
    .count()
    .drop(col("category"))

  val weblogsAggDF: Dataset[Row] = weblogsCleanDF
    .groupBy(col("uidw"), col("url"))
    .agg(
      count(col("url")).as("count_url"))

  val urlWithDomainPrep: Dataset[Row] = weblogsAggDF
    .join(domainCatsCleanDF, weblogsAggDF.col("url").equalTo(domainCatsCleanDF("domain")), "left")
    .select(col("uidw"), col("url"), col("count_url"), col("category"))

  val urlWithDomain: Dataset[Row] = urlWithDomainPrep
    .groupBy(col("uidw"))
    .pivot(col("category"))
    .sum("count_url")
    .drop(col("category"))

  val resultDF: Dataset[Row] = clientsCleanDF
    .join(visitsAggDF, clientsCleanDF.col("uid").equalTo(visitsAggDF("uidv")), "left")
    .join(urlWithDomain, clientsCleanDF.col("uid").equalTo(urlWithDomain("uidw")), "left")
    .drop(col("uidv"))
    .drop(col("uidw"))
    .na.fill(0)

// Save report in PostgreSQL
  resultDF
    .write
    .format("jdbc")
    .option("url", s"jdbc:postgresql://${hostPg}/${schemaPg}")
    .option("user", s"${usernamePg}")
    .option("password", s"${passwordPg}")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", s"${targetTable}")
    .mode("overwrite")
    .saveAsTable(s"${targetTable}")
}

