package com.rec

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object RecSystem extends App with RecWrapper {

  val salesOrdersDf = buildSalesOrders("sales/PastWeaponSalesOrders.csv")
  salesOrdersDf.show
  salesOrdersDf.printSchema

  import session.implicits._

  val ratingsDf: DataFrame = salesOrdersDf.map( salesOrder =>
    Rating( salesOrder.getInt(0),
      salesOrder.getInt(2),
      salesOrder.getDouble(6)
    ) ).toDF("user", "item", "rating")

  val ratings: RDD[Rating] = ratingsDf.rdd.map(row =>
    Rating(row.getInt(0),
      row.getInt(1),
      row.getDouble(2)))

  println("Ratings RDD is: " + ratings.take(10).mkString(" "))

  val ratingsModel: MatrixFactorizationModel = ALS.train(ratings,
    6, /* THE RANK */
    10, /* Number of iterations */
    15.0 /* Lambda, or regularization parameter */
  )

  val weaponSalesLeadDf = buildSalesLeads("sales/WeaponSalesLeads.csv")
  println("Weapons Sales Lead dataframe is: ")
  weaponSalesLeadDf.show

  val customerWeaponsSystemPairDf: DataFrame = weaponSalesLeadDf.map(salesLead => ( salesLead.getInt(0), salesLead.getInt(2) )).toDF("user","item")
  println("The Customer-Weapons System dataframe as tuple pairs looks like: ")
  customerWeaponsSystemPairDf.show

  val customerWeaponsSystemPairRDD: RDD[(Int, Int)] = customerWeaponsSystemPairDf.rdd.map(row => (row.getInt(0), row.getInt(1)))

  val weaponRecs: RDD[Rating] = ratingsModel.predict(customerWeaponsSystemPairRDD).distinct()
  println("Future ratings are: " + weaponRecs.foreach(rating => println("Customer: " + rating.user + " Product:  " + rating.product + " Rating: " + rating.rating)))


}
