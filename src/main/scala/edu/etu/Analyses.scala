package edu.etu

import edu.etu.database.DatabaseConnection
import org.apache.spark.sql.functions._
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

    val join_df = spark.read.format("mongodb")
      .option("spark.mongodb.read.connection.uri", s"mongodb+srv://tuna:ouz" +
        s"@supplychaindbread.8rqcr4x.mongodb.net/test.country_ico")
      .load()
      .join(read_df, "country")

    val write_df = join_df
      .filter(col("real") < col("scheduled"))
      .groupBy("ioc")
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

    spark.close()
  }

  def changesOfCustomersOrderCountByMonth(): Unit = {
    val collection = "order_count_by_date"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "date: {$toDate: '$order date (DateOrders)'}" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df = read_df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
      .withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))

    read_df = read_df.withColumn("month",
      when(read_df("month") === 1, "01")
        .when(read_df("month") === 2, "02")
        .when(read_df("month") === 3, "03")
        .when(read_df("month") === 4, "04")
        .when(read_df("month") === 5, "05")
        .when(read_df("month") === 6, "06")
        .when(read_df("month") === 7, "07")
        .when(read_df("month") === 8, "08")
        .when(read_df("month") === 9, "09")
        .otherwise(col("month"))
    )

    val write_df = read_df
      .withColumn("month_year", concat(col("year"), lit("-"), col("month")))
      .groupBy("month_year")
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

  def benefitPerOrderAnalysesBasedOnDiscountAndCategory(): Unit = { // Max avg earnings analysis by discount rate and category

    val collection = "sales_per_customer_change_by_discount"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "discount_rate: '$Order Item Discount Rate'" +
      "category: '$Category Name'" +
      "sales_per_customer: '$Sales per customer'" +
      "status: '$Order Status'" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    read_df = read_df.withColumn("discount_rate",
      when(col("discount_rate") > 0 && col("discount_rate") < 0.05, "0.00-0.05")
      .when(col("discount_rate") >= 0.05 && col("discount_rate") < 0.1,"0.05-0.10")
      .when(col("discount_rate") >= 0.1 && col("discount_rate") < 0.15,"0.10-0.15")
      .when(col("discount_rate") >= 0.15 && col("discount_rate") < 0.2, "0.15-0.20")
      .when(col("discount_rate") >= 0.2 && col("discount_rate") <= 0.25, "0.20-0.25")
      .otherwise("%0")
    )

    val write_df = read_df
      .filter(col("status") === "COMPLETE")
      .groupBy("category", "discount_rate")
      .avg("sales_per_customer")
      .withColumnRenamed("avg(sales_per_customer)","avg_sale_per_customer")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }
  def customerOrderCycle(): Unit = {
    val collection = "customer_order_cycle"
    val spark = db.createSparkSession(collection)
    val pipeline = "{ $project: { " +
      "real: '$Days for shipping (real)'" +
      "status: '$Order Status'" +
      "ship_date: {$toDate: '$shipping date (DateOrders)'}" +
      "order_date: {$toDate: '$order date (DateOrders)'}" +
      "} }"

    var read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .filter(col("status") =!= "CANCELED")
      .withColumn("diff_in_days", datediff(col("ship_date"), col("order_date")))
      .filter(col("diff_in_days") >= 0)
      .agg(avg(col("diff_in_days") + col("real")).as("customer_order_cycle"))


    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()

    spark.close()
  }

  def supplierResponseTime(): Unit = {
    val collection = "supplier_response_time"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "ship_date: {$toDate: '$shipping date (DateOrders)'}" +
      "order_date: {$toDate: '$order date (DateOrders)'}" +
      "status: '$Order Status'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val write_df = read_df
      .filter(col("status") =!= "CANCELED")
      .withColumn("diff_in_days", datediff(col("ship_date"), col("order_date")))
      .filter(col("diff_in_days") >= 0)
      .agg(avg("diff_in_days").as("supplier_response_time"))

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()
  }

  def perfectOrderIndex(): Unit = {
    val collection = "perfect_order_index"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "status: '$Order Status'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val errors_df = read_df
      .groupBy("status")
      .count()
      .filter(col("status") === "CANCELED" || col("status") === "SUSPECTED_FRAUD")
      .agg(sum("count").as("count_error"))

    val error_free_df = read_df
      .groupBy("status")
      .count()
      .filter(col("status") === "COMPLETE" || col("status") === "CLOSED")
      .agg(sum("count").as("count_err_free"))

    val write_df = errors_df.join(error_free_df)
      .withColumn("perfect_order_index",
        lit(100) * col("count_err_free") / (col("count_err_free") + col("count_error"))
      )
      .select("perfect_order_index")

    write_df.write.format("mongodb")
      .mode("append")
      .option("maxBatchSize", 2048)
      .option("operationType", "insert")
      .option("writeConcern.w", 0)
      .option("writeConcern.journal", false)
      .save()
  }

  def lateShippingAnalysisBasedOnCustomerCountryPercentage(): Unit = { // Late shipping analysis by customers' country

    val collection = "late_ship_customer_country_perc"
    val spark = db.createSparkSession(collection)

    val pipeline = "{ $project: { " +
      "country: '$Order Country'," +
      "scheduled: '$Days for shipment (scheduled)'," +
      "real: '$Days for shipping (real)'" +
      "} }"

    val read_df = spark.read.format("mongodb")
      .option("aggregation.pipeline", pipeline)
      .load()

    val join_df = spark.read.format("mongodb")
      .option("spark.mongodb.read.connection.uri", s"mongodb+srv://tuna:ouz" +
        s"@supplychaindbread.8rqcr4x.mongodb.net/test.country_ico")
      .load()
      .join(read_df, "country")

    val late_df = join_df
      .filter(col("real") < col("scheduled"))
      .groupBy("ioc")
      .count()
      .withColumnRenamed("count", "late")

    val total_df = join_df
      .groupBy("ioc")
      .count()
      .withColumnRenamed("count", "total")

    val write_df = late_df.join(total_df, "ioc")
      .withColumn("late_percentage", (lit(100) * col("late") / col("total")).cast(DecimalType(10,2)))
      .select("ioc", "late_percentage")


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
