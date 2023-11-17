package edu.etu.database

import org.apache.spark.sql.SparkSession

class DatabaseConnection() {
  def getConnectedSparkSession: SparkSession = {
    return SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", s"mongodb+srv://tuna:ouz@supplychainanalysis.bdwzwi6.mongodb.net/supply_chain_db.supply_chain")
      .config("spark.mongodb.write.connection.uri", s"mongodb+srv://tuna:ouz@supplychainanalysis.bdwzwi6.mongodb.net/supply_chain_db.supply_chain")
      .getOrCreate()
  }

}
