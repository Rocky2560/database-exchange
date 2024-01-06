package spark

import java.util.Properties

import com.ekbana.db.bootstrap.Launcher
import com.ekbana.db.concrete_classes.HSQLDBConnection
import org.apache.spark.sql.{DataFrame, SparkSession}

class CreateDF {

  def json_to_df(values:String):DataFrame = {
    val sp:SparkSession = Launcher.sp
    val sc = sp.sparkContext
    val sqlContext = sp.sqlContext
    val rdd = sc.parallelize(Seq(values))
    val df = sqlContext.read.json(rdd)
    df
  }

  def read_from_shql(): DataFrame = {
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    val sp: SparkSession = Launcher.sp
    val pp: Properties = new Properties()
    pp.setProperty("username", HSQLDBConnection.getUSER)
    pp.setProperty("password", HSQLDBConnection.getPASS)
    val df_site= sp.read
      //        .format("org.hsqldb.jdbc.JDBCDriver")
      .format("jdbc")
      .option("url", HSQLDBConnection.getDB_URL)
      .option("dbtable", "V_SITESALE")
      .load()
    //      .jdbc(HSQLDBConnection.getDB_URL,"V_SITESALE",pp)
    df_site.show()
    df_site.printSchema()
    df_site
  }

}
