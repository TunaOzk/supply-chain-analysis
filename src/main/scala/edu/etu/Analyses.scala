package edu.etu

import org.apache.spark.sql.SparkSession

class Analyses(sparkSession : SparkSession) {
  def readAllData(): Unit = {
    val read_df = sparkSession.read.format("mongodb").load()
    read_df.show()
  }

}
