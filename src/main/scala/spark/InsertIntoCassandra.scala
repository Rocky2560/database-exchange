package spark

import com.ekbana.db.bootstrap.Launcher
import com.ekbana.db.config.ConnectorConfig
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class InsertIntoCassandra {
  val connnectorConfig:ConnectorConfig=Launcher.connector_config
  val config:Config=connnectorConfig.config
  val create_df:CreateDF = new CreateDF()
  val sp:SparkSession = Launcher.sp
  import sp.implicits._

  def insertData(df: DataFrame, table: String, keyspace: String) : Unit = {
    try{
      df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .option("table", table)
        .option("keyspace", keyspace)
        .option("spark.cassandra.connection.host", connnectorConfig.getCassandra_host)
        .option("spark.cassandra.connection.port", connnectorConfig.getCassandra_port)
        .option("spark.cassandra.auth.username", connnectorConfig.getCassandra_user)
        .option("spark.cassandra.auth.password", connnectorConfig.getCassandra_pass)
        .option("spark.cassandra.connection.ssl.clientAuth.enabled", true)
        .option("spark.cassandra.connection.ssl.enabled", true)
        .option("spark.cassandra.connection.ssl.keyStore.path", connnectorConfig.getKEYSTORE_PATH)
        .option("spark.cassandra.connection.ssl.keyStore.password", connnectorConfig.getKEYSTORE_PASSWORD)
        .option("spark.cassandra.connection.ssl.trustStore.path", connnectorConfig.getTRUSTSTORE_PATH)
        .option("spark.cassandra.connection.ssl.trustStore.password", connnectorConfig.getTRUSTSTORE_PASSWORD)
        .save()
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def to_csv(df:DataFrame): Unit ={
    try{

    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoSales(dff:DataFrame, table:String, keyspace:String): Unit = {
    try{
      val df = dff
        .withColumn("savings", col = $"promoamt" + $"toataldiscountamt" + $"loyaltydiscount")
        .withColumn("tagged", lit("n"))

      insertData(df, table, keyspace)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  def insertIntoCollection(dff:DataFrame, table:String, keyspace:String): Unit = {
    try{
      var df = dff
      df = df
        .withColumn("billyear", year(col("billdate").cast("date")))
        .withColumn("tagged", lit("n"))
      insertData(df, table, keyspace)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoCustomer(dff:DataFrame, table:String, keyspace:String): Unit = {
    try{
      var df = dff
      df = df.na.fill("N/A", Seq(config.getString("cust.fill_na")))
      insertData(df, table, keyspace)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoSites(df:DataFrame, table:String, keyspace:String): Unit = {
    try{
      insertData(df, table, keyspace)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertIntoItems(df:DataFrame, table:String, keyspace:String): Unit = {
    try{
      insertData(df, table, keyspace)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def insertCount(jsonData: String, table: String, keyspace: String) : Unit = {
    try{
      val df = create_df.json_to_df(jsonData)
      insertData(df, table, keyspace)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}