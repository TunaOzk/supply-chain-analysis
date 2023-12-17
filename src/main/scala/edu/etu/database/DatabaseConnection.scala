package edu.etu.database

import org.apache.spark.sql.SparkSession

class DatabaseConnection() {
  def createSparkSession(w_collection_name: String) : SparkSession = {
    return SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", s"mongodb+srv://tuna:ouz@supplychaindbread.8rqcr4x.mongodb.net/test.supply_chain_db_r")
      .config("spark.mongodb.write.connection.uri", s"mongodb+srv://tuna:ouz@supplychaindbwrite.nw5uhcd.mongodb.net/calculations."+w_collection_name)
      .getOrCreate()
  }

  def createSparkSessionForAccessLogs(w_collection_name: String): SparkSession = {
    return SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", s"mongodb+srv://tuna:ouz@supplychaindbread.8rqcr4x.mongodb.net/test.supply_chain_access_logs")
      .config("spark.mongodb.write.connection.uri", s"mongodb+srv://tuna:ouz@supplychaindbwrite.nw5uhcd.mongodb.net/calculations." + w_collection_name)
      .getOrCreate()
  }

}
