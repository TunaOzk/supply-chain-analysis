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
    val analyser = new Analyses(db)

//    analyser.readAllData()
    analyser.lateShippingAnalysisBasedOnCustomerCountry()
    analyser.lateShippingAnalysisBasedOnCustomerCity()
    analyser.productCategoryAnalysesBasedOnCustomerCountryAndCategory()
    analyser.averageProductPriceAnalysesBasedOnCustomerCityAndCategory()
    analyser.benefitPerOrderAnalysesBasedOnStoreCityAndCategory()
    analyser.mostGivenOrdersAnalysesBasedOnStoreCity()
    analyser.benefitPerOrderAnalysesBasedOnDiscountAndCategory()
    analyser.benefitPerOrderAnalysesBasedOnCategory()
    analyser.orderTimeBasedOnCustomerSegment()
    analyser.categoryOrderAnalysisBasedOnHour()
    analyser.categoryOrderBasedOnMonth()
    analyser.avgShippingAnalysisBasedOnCustomerCountry()
    analyser.changesOfCustomersOrderCountByMonth()

  }

}
