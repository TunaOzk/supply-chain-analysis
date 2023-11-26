package edu.etu

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class Analyses(sparkSession : SparkSession) {
  def readAllData(): Unit = {
    val read_df = sparkSession.read.format("mongodb").load()
    read_df.show()
  }

  def lateShippingAnalysisBasedOnCustomerCountry(): Unit = {      // Late shipping analyze by customers' country
    val pipeline = "{ $project: { " +
      "_id: 0," +
      "country: '$Order Country'," +
      "scheduled: '$Days for shipment (scheduled)'," +
      "real: '$Days for shipping (real)'" +
      "} }"

    val read_df = sparkSession.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df
      .filter(col("real") < col("scheduled"))
      .groupBy("country")
      .count()
      .show()
  }

  def lateShippingAnalysisBasedOnCustomerCity(): Unit = {       // Late shipping analyze by customers' city
    val pipeline = "{ $project: { " +
      "_id: 0," +
      "city: '$Order City'," +
      "scheduled: '$Days for shipment (scheduled)'," +
      "real: '$Days for shipping (real)'" +
      "} }"

    val read_df = sparkSession.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df
      .filter(col("real") < col("scheduled"))
      .groupBy("city")
      .count()
      .show()
  }

  def productCategoryAnalysesBasedOnCustomerCity(): Unit = {    // Product category analyze by customers' city
    val pipeline = "{ $project: { " +
      "_id: 0," +
      "city: '$Order City'," +
      "category: '$Category Name'" +
      "} }"

    val read_df = sparkSession.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df
      .groupBy("city", "category")
      .count()
      .show()
  }
}
