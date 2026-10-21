package com.ecommerce.models

case class Merchant(
  merchant_id: String,
  name: String,
  category: String,
  region: String,
  commission_rate: Option[Double],
  establishment_date: String // Format: yyyyMMdd
)

case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],
  timestamp: String, // Format: yyyyMMddHHmmss
  location: String,
  payment_method: String,
  category: String
)

case class User(
  user_id: String,
  age: Option[Int],
  annual_income: Option[Double],
  city: String,
  customer_segment: String,
  preferred_categories: Seq[String],
  registration_date: String // Format: yyyyMMdd
)

case class ProductItem(
  product_id: String,
  name: String,
  category: String,
  price: Option[Double],
  merchant_id: String,
  rating: Option[Double],
  stock: Option[Int]
)

case class ValidationResult[T](
  valid: org.apache.spark.sql.Dataset[T],
  rejected: org.apache.spark.sql.DataFrame
)

case class QualityMetrics(
  dataset: String,
  nb_lignes_lues: Long,
  nb_lignes_valides: Long,
  nb_lignes_rejetees: Long,
  taux_rejet: Double,
  nb_valeurs_nulles: Long
)
