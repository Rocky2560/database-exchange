package spark

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import com.ekbana.db.bootstrap.Launcher
import com.ekbana.db.config.ConnectorConfig
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.util.Try

class SaveToCSV {
  val connnectorConfig:ConnectorConfig=Launcher.connector_config
  val config:Config=connnectorConfig.config

  def getListOfFiles(dir: String):String = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.getName.startsWith("part-0000")).toList.last.getPath
    } else {
      ""
    }
  }


  def mv(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)


  def save(df: DataFrame, source: String, dateTime: String) : Unit = {
    try{
      val frmt = new SimpleDateFormat("dd-MMM-yy")
      val cal = Calendar.getInstance
      val parsedDate = frmt.parse(dateTime)
      cal.setTime(parsedDate)

      val year = new SimpleDateFormat("yyyy").format(cal.getTime)
      val month = new SimpleDateFormat("MM").format(cal.getTime)
      val day = new SimpleDateFormat("dd").format(cal.getTime)

      val year_month = year.concat("/").concat(month)
      val sourceFolder = source + "/" + year_month

      df.coalesce(1).write.option("header", "true").mode("append").csv("file://" + sourceFolder)

      val sourceFilename = getListOfFiles(sourceFolder)
      val destinationFilename = sourceFolder + "/" + day + ".csv"

      if(!sourceFilename.equals("")){
        mv(sourceFilename, destinationFilename)
      }
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}