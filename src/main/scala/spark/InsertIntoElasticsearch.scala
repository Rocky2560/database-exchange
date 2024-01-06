package spark

import com.ekbana.db.bootstrap.Launcher
import com.ekbana.db.config.ConnectorConfig
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class InsertIntoElasticsearch {
  val connnectorConfig:ConnectorConfig=Launcher.connector_config
  val config:Config=connnectorConfig.config
  val sp:SparkSession=Launcher.sp
  import sp.implicits._

  def insertData(df: DataFrame, table: String) : Unit = {
    try{
      df.write
        .format("org.elasticsearch.spark.sql")
        .mode("append")
        .option("es.resource", table)
        .option("es.nodes", config.getString("es_nodes"))
        .option("es.port", config.getInt("es_port"))
        .option("es.net.http.auth.user", config.getString("es_user"))
        .option("es.net.http.auth.pass", config.getString("es_pass"))
        .option("es.net.ssl", "true")
        .option("es.nodes.wan.only", "true")
        .option("es.net.ssl.protocol", "SSL")
        .save()
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoSales(dff:DataFrame, table:String): Unit = {
    try{
      var df = dff
      df = df.drop("id")
      df = df
        .withColumn("billdate", to_timestamp(col("billdate"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("date", date_format(col("billdate"), "yyyy-MM-dd"))
        .withColumn("savings", col = $"promoamt" + $"toataldiscountamt" + $"loyaltydiscount")
        .withColumn("tagged", lit("n"))

      println("\n########## Inserting sales data ############\n")
      insertData(df, config.getString("sales.es_sales_index"))

      df = df.dropDuplicates("billdate", "billno", "admsite_code")
      df = df
        .select("admsite_code", "division", "section", "department", "cat1", "icode", "saleamt", "netamt", "billyear", "date")

      println("\n########## Inserting billdetails ############\n")
      insertData(df, config.getString("sales.es_billdetails"))

    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoCollection(dff:DataFrame, table:String): Unit = {
    try{
      var df = dff
      df = df.drop("id").drop("createdon")
      df = df
        .withColumn("billdate", to_timestamp(col("billdate"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("tagged", lit("n"))
      insertData(df, config.getString("sales.es_collection_index"))
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoItems(dff:DataFrame, table:String): Unit = {
    try{
      var df = dff
      insertData(df, config.getString("item.es_index"))
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoCustomers(dff:DataFrame, table:String): Unit = {
    try{
      var df = dff
      df = df.drop("id")
      insertData(df, config.getString("cust.es_index"))
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoSites(dff:DataFrame, table:String): Unit = {
    try{
      var df = dff
      df = df.drop("id")
      insertData(df, table)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}