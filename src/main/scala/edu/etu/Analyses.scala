package edu.etu

import edu.etu.database.DatabaseConnection
import org.apache.spark.sql.functions.{avg, col, date_format, hour, max, month, to_date, when}
import org.apache.spark.sql.types.DecimalType

class Analyses(db: DatabaseConnection) {
  def lateShippingAnalysisBasedOnCustomerCountry(): Unit = {      // Late shipping analysis by customers' country

    val collection = "late_ship_customer_country"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "country: '$Order Country'," +
      "scheduled: '$Days for shipment (scheduled)'," +
      "real: '$Days for shipping (real)'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .filter(col("real") < col("scheduled"))
      .groupBy("country")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def avgShippingAnalysisBasedOnCustomerCountry(): Unit = {   // avg days of late shipment analysis based on country
    val collection = "avg_shipment_customer_country"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "country: '$Order Country'," +
      "scheduled: '$Days for shipment (scheduled)'," +
      "real: '$Days for shipping (real)'" +
      "real_minus_scheduled: { $subtract: ['$Days for shipping (real)', '$Days for shipment (scheduled)'] }" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .groupBy("country")
      .avg()
      .withColumnRenamed("avg(real)", "avg_real")
      .withColumnRenamed("avg(scheduled)", "avg_scheduled")
      .withColumnRenamed("avg(real_minus_scheduled)", "real_minus_scheduled")
      .filter(col("avg(scheduled)") < col("avg(real)"))

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def lateShippingAnalysisBasedOnCustomerCity(): Unit = {       // Late shipping analysis by customers' city

    val collection = "late_ship_customer_city"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "city: '$Order City'," +
      "scheduled: '$Days for shipment (scheduled)'," +
      "real: '$Days for shipping (real)'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .filter(col("real") < col("scheduled"))
      .groupBy("city")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def productCategoryAnalysesBasedOnCustomerCountryAndCategory(): Unit = {    // Product category analysis by customers' country

    val collection = "prod_category_customer_country"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "country: '$Order Country'," +
      "category: '$Category Name'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .groupBy("country", "category")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def averageProductPriceAnalysesBasedOnCustomerCityAndCategory(): Unit = {  // Avg price analysis by customers' country

    val collection = "avg_price_customer_country"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "country: '$Order Country'," +
      "category: '$Category Name'" +
      "price: '$Product Price'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .groupBy("country", "category")
      .agg(avg("price").cast(DecimalType(10,2)).as("avg_price"))
      .sort("country")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def benefitPerOrderAnalysesBasedOnStoreCityAndCategory(): Unit = { // Avg earnings analysis by city and category

    val collection = "benefit_store_city"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "city: '$Customer City'," +
      "category: '$Category Name'" +
      "benefit: '$Benefit per order'" +
      "status: '$Order Status'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .filter(col("status") === "COMPLETE")
      .groupBy("city", "category")
      .agg(avg("benefit").cast(DecimalType(10,2)).as("benefit"))
      .sort("city")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def mostGivenOrdersAnalysesBasedOnStoreCity(): Unit = { // Number of given orders by customers analysis by store loc.

    val collection = "most_given_order_store_city"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "store_city: '$Customer City'," +
      "customer_city: '$Order City'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .groupBy("store_city")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def benefitPerOrderAnalysesBasedOnCategory(): Unit = {    // Benefit per order analysis by product category
    val collection = "benefit_category"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "category: '$Category Name'" +
      "benefit: '$Benefit per order'" +
      "status: '$Order Status'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .filter(col("status") === "COMPLETE")
      .groupBy("category")
      .agg(avg("benefit").cast(DecimalType(10, 2)).as("benefit"))

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def orderTimeBasedOnCustomerSegment(): Unit = {       // order hour analysis by customer type
    val collection = "time_segment"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "segment: '$Customer Segment'" +
      "time: {$toDate: '$order date (DateOrders)'}" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df = read_df.withColumn("time", date_format(col("time"), "HH:mm"))
    read_df = read_df.withColumn("time",
      when(hour(col("time")).between(0, 2), "00-02")
      .when(hour(col("time")).between(2, 4), "02-04")
      .when(hour(col("time")).between(4, 6), "04-06")
      .when(hour(col("time")).between(6, 8), "06-08")
      .when(hour(col("time")).between(8, 10), "08-10")
      .when(hour(col("time")).between(10, 12), "10-12")
      .when(hour(col("time")).between(12, 14), "12-14")
      .when(hour(col("time")).between(14, 16), "14-16")
      .when(hour(col("time")).between(16, 18), "16-18")
      .when(hour(col("time")).between(18, 20), "18-20")
      .when(hour(col("time")).between(20, 22), "20-22")
      .when(hour(col("time")).between(22, 24), "22-24"))

    val write_df = read_df
      .groupBy("segment", "time")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def categoryOrderAnalysisBasedOnHour() : Unit = {      // Order count analysis by category and hour
    val collection = "time_category"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "category: '$Category Name'" +
      "time: {$toDate: '$order date (DateOrders)'}" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df = read_df.withColumn("time", date_format(col("time"), "HH:mm"))
    read_df = read_df.withColumn("time",
      when(hour(col("time")).between(0, 2), "00-02")
        .when(hour(col("time")).between(2, 4), "02-04")
        .when(hour(col("time")).between(4, 6), "04-06")
        .when(hour(col("time")).between(6, 8), "06-08")
        .when(hour(col("time")).between(8, 10), "08-10")
        .when(hour(col("time")).between(10, 12), "10-12")
        .when(hour(col("time")).between(12, 14), "12-14")
        .when(hour(col("time")).between(14, 16), "14-16")
        .when(hour(col("time")).between(16, 18), "16-18")
        .when(hour(col("time")).between(18, 20), "18-20")
        .when(hour(col("time")).between(20, 22), "20-22")
        .when(hour(col("time")).between(22, 24), "22-24"))

    val write_df = read_df
      .groupBy("time", "category")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def categoryOrderBasedOnMonth() : Unit = {   // order count analysis by category and  month
    val collection = "month_category"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "category: '$Category Name', " +
      "month: {$toDate: '$order date (DateOrders)'}" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df = read_df.withColumn("month", to_date(col("month"), "MM/dd/yyyy"))
    read_df = read_df.withColumn("month", month(col("month")))

    val write_df = read_df
      .groupBy("month", "category")
      .count()
      .withColumnRenamed("count", "n_count")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()
  }

  def benefitPerOrderAnalysesBasedOnDiscountAndCategory(): Unit = { // Max avg earnings analysis by discount rate and category

    val collection = "benefit_discount_category"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "benefit: '$Benefit per order'," +
      "discount_rate: '$Order Item Discount Rate'" +
      "category: '$Category Name'" +
      "status: '$Order Status'" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df = read_df.withColumn("discount_rate",
      when(read_df("discount_rate") > 0 && read_df("discount_rate") < 0.05, "0-0.05")
      .when(read_df("discount_rate") >= 0.05 && read_df("discount_rate") < 0.1,"0.05-0.1")
      .when(read_df("discount_rate") >= 0.1 && read_df("discount_rate") < 0.15,"0.1-0.15")
      .when(read_df("discount_rate") >= 0.15 && read_df("discount_rate") < 0.2, "0.15-0.2")
      .when(read_df("discount_rate") >= 0.2 && read_df("discount_rate") <= 0.25, "0.2-0.25")
    ).filter(col("discount_rate").isNotNull)

    val write_df = read_df
      .filter(col("status") === "COMPLETE")
      .groupBy("category", "discount_rate")
      .agg(avg("benefit").cast(DecimalType(10,2)).as("benefit"))
      .groupBy("category", "discount_rate")
      .agg(max("benefit"))

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }
}
