package edu.etu

import org.apache.spark.sql.SparkSession

class Analyses(sparkSession : SparkSession) {
  def readAllData(): Unit = {
    val read_df = sparkSession.read.format("mongodb").load()
  }

  def lateShippingAnalysisBasedOnCountry(): Unit = {
    val pipeline = "{'$project': { _id: 0, 'Type' : 1, 'Days for shipping (real)': 1, 'Order Country': 1, 'Days for shipment (scheduled)': 1}}"
    val read_df = sparkSession.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()
      .withColumnRenamed("Days for shipping (real)", "real")
      .withColumnRenamed("Days for shipment (scheduled)", "scheduled")


    import org.apache.spark.sql.functions.col
    read_df
      .filter(col("real") < col("scheduled"))
      .groupBy("Order Country")
      .count()
      .show()

  }
}
