package com.ecommerce.ingestion

import com.ecommerce.models._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataIngestion {

  def loadTransactions(path: String)(implicit spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._

    val transactionSchema = StructType(Seq(
      StructField("transaction_id", StringType, nullable = true),
      StructField("user_id",        StringType, nullable = true),
      StructField("product_id",     StringType, nullable = true),
      StructField("merchant_id",    StringType, nullable = true),
      StructField("amount",         DoubleType, nullable = true),
      StructField("timestamp",      StringType, nullable = true),
      StructField("location",       StringType, nullable = true),
      StructField("payment_method", StringType, nullable = true),
      StructField("category",       StringType, nullable = true)
    ))

    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(transactionSchema)
      .csv(path)
      .as[Transaction]
  }

  def loadUsers(path: String)(implicit spark: SparkSession): Dataset[User] = {
    import spark.implicits._

    spark.read
      .option("mode", "PERMISSIVE")
      .json(path)
      .select(
        col("user_id"),
        col("age").cast(IntegerType),
        col("annual_income").cast(DoubleType),
        col("city"),
        col("customer_segment"),
        coalesce(col("preferred_categories"), array().cast(ArrayType(StringType))).as("preferred_categories"),
        col("registration_date")
      )
      .as[User]
  }

  def loadProducts(path: String)(implicit spark: SparkSession): Dataset[ProductItem] = {
    import spark.implicits._

    spark.read
      .parquet(path)
      .select(
        col("product_id"),
        col("name"),
        col("category"),
        col("price").cast(DoubleType),
        col("merchant_id"),
        col("rating").cast(DoubleType),
        col("stock").cast(IntegerType)
      )
      .as[ProductItem]
  }

  def loadMerchants(path: String)(implicit spark: SparkSession): Dataset[Merchant] = {
    import spark.implicits._

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select(
        col("merchant_id"),
        col("name"),
        col("category"),
        col("region"),
        col("commission_rate").cast(DoubleType),
        col("establishment_date")
      )
      .as[Merchant]
  }
}
