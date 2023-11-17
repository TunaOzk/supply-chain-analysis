package edu.etu

import edu.etu.database.DatabaseConnection
import org.apache.log4j.{Level, Logger}

/**
 * @author ${user.name}
 */
object App {
  Logger.getLogger("org").setLevel(Level.OFF) // to suppress redundant logger info
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args : Array[String]): Unit = {
    val db = new DatabaseConnection()
    val sparkSession = db.getConnectedSparkSession
    val analyser = new Analyses(sparkSession)

    analyser.readAllData()

  }

}
