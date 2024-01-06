package spark

import com.ekbana.db.bootstrap.Launcher
import com.ekbana.db.config.ConnectorConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


class CreateSparkConnection {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val config:ConnectorConfig=Launcher.connector_config

  val cassandra_spark:SparkSession =
    SparkSession
      .builder()
      .appName("Cassandra Spark")
      .master(config.getSpark_master)
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()

  val hsqldb_spark:SparkSession =
    SparkSession
      .builder()
      .appName("hsql Spark")
      .master(config.getSpark_master)
      .config("spark.driver.allowMultipleContexts", "true")
      //      .config("spark.sql.session.timezone", "utc")
      .getOrCreate()
}

