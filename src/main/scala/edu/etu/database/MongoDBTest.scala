package edu.etu.database

import org.apache.log4j.{Level, Logger}

object MongoDBTest {
  Logger.getLogger("org").setLevel(Level.OFF)   // to suppress redundant logger info
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val db = new DatabaseConnection()
    val spark = db.getConnectedSparkSession

    val read_df = spark.read.format("mongodb").load()
    read_df.show()

    val dummy_json = "{\"Type\":\"DENEME\",\"Days for shipping (real)\":{\"$numberInt\":\"4\"},\"Days for shipment (scheduled)\":{\"$numberInt\":\"4\"},\"Benefit per order\":{\"$numberDouble\":\"-247.7799988\"},\"Sales per customer\":{\"$numberDouble\":\"309.7200012\"},\"Delivery Status\":\"Shipping on time\",\"Late_delivery_risk\":{\"$numberInt\":\"0\"},\"Category Id\":{\"$numberInt\":\"73\"},\"Category Name\":\"Sporting Goods\",\"Customer City\":\"San Jose\",\"Customer Country\":\"EE. UU.\",\"Customer Email\":\"XXXXXXXXX\",\"Customer Fname\":\"Gillian\",\"Customer Id\":{\"$numberInt\":\"19491\"},\"Customer Lname\":\"Maldonado\",\"Customer Password\":\"XXXXXXXXX\",\"Customer Segment\":\"Consumer\",\"Customer State\":\"CA\",\"Customer Street\":\"8510 Round Bear Gate\",\"Customer Zipcode\":{\"$numberInt\":\"95125\"},\"Department Id\":{\"$numberInt\":\"2\"},\"Department Name\":\"Fitness\",\"Latitude\":{\"$numberDouble\":\"37.29223251\"},\"Longitude\":{\"$numberDouble\":\"-121.881279\"},\"Market\":\"Pacific Asia\",\"Order City\":\"Bikaner\",\"Order Country\":\"India\",\"Order Customer Id\":{\"$numberInt\":\"19491\"},\"order date (DateOrders)\":\"1/13/2018 12:06\",\"Order Id\":{\"$numberInt\":\"75938\"},\"Order Item Cardprod Id\":{\"$numberInt\":\"1360\"},\"Order Item Discount\":{\"$numberDouble\":\"18.03000069\"},\"Order Item Discount Rate\":{\"$numberDouble\":\"0.059999999\"},\"Order Item Id\":{\"$numberInt\":\"179253\"},\"Order Item Product Price\":{\"$numberDouble\":\"327.75\"},\"Order Item Profit Ratio\":{\"$numberDouble\":\"-0.800000012\"},\"Order Item Quantity\":{\"$numberInt\":\"1\"},\"Sales\":{\"$numberDouble\":\"327.75\"},\"Order Item Total\":{\"$numberDouble\":\"309.7200012\"},\"Order Profit Per Order\":{\"$numberDouble\":\"-247.7799988\"},\"Order Region\":\"South Asia\",\"Order State\":\"Rajastán\",\"Order Status\":\"CLOSED\",\"Order Zipcode\":\"\",\"Product Card Id\":{\"$numberInt\":\"1360\"},\"Product Category Id\":{\"$numberInt\":\"73\"},\"Product Description\":\"\",\"Product Image\":\"http://images.acmesports.sports/Smart+watch\",\"Product Name\":\"Smart watch\",\"Product Price\":{\"$numberDouble\":\"327.75\"},\"Product Status\":{\"$numberInt\":\"0\"},\"shipping date (DateOrders)\":\"1/17/2018 12:06\",\"Shipping Mode\":\"Standard Class\"}"
    import spark.implicits._
    val write_ds = Seq(dummy_json).toDS()
    val write_df = spark.read.json(write_ds)
    write_df.show()

    write_df.write.format("mongodb").mode("append").save()
  }

}
