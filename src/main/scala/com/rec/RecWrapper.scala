package com.rec

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait RecWrapper {

  val session = SparkSession
    .builder()
    .master("local")
    .appName("Recommendation System")
    .getOrCreate()

  val salesOrderSchema: StructType = StructType(Array(
    StructField("CustomerId", IntegerType,false),
    StructField("CustomerName", StringType,false),
    StructField("ItemId", IntegerType,true),
    StructField("ItemName",  StringType,true),
    StructField("ItemUnitPrice",DoubleType,true),
    StructField("OrderSize", DoubleType,true),
    StructField("AmountPaid",  DoubleType,true)
  ))

  val salesLeadSchema: StructType = StructType(Array(
    StructField("CustomerId", IntegerType,false),
    StructField("CustomerName", StringType,false),
    StructField("ItemId", IntegerType,true),
    StructField("ItemName",  StringType,true)
  ))

  def buildSalesOrders(dataSet: String): DataFrame = {
    session.read
      .format("com.databricks.spark.csv")
      .option("header", true).schema(salesOrderSchema).option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .load(dataSet).cache()
  }

  def buildSalesLeads(dataSet: String): DataFrame = {
    session.read
      .format("com.databricks.spark.csv")
      .option("header", true).schema(salesLeadSchema).option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .load(dataSet).cache()
  }
}
