package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class DataTransformation(spark: SparkSession) {

  /** Joint les 4 tables, ajoute les features temporelles, le rang, le total et la tranche d'age. */
  def enrichTransactionData(transactions: DataFrame, users: DataFrame,
                            products: DataFrame, merchants: DataFrame): DataFrame = {

    // 1. On ne garde que les colonnes utiles, avec des noms sans ambiguite
    val u = users.select("user_id", "age", "annual_income", "city", "customer_segment")
    val p = products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      col("price").as("product_price"),
      col("rating").as("product_rating"))
    val m = merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region").as("merchant_region"),
      col("commission_rate"))

    // 2. Jointures left : on garde toutes les transactions, meme sans correspondance
    val jointes = transactions
      .join(u, Seq("user_id"), "left")
      .join(p, Seq("product_id"), "left")
      .join(m, Seq("merchant_id"), "left")

    // 3. Date reelle + UDF, puis on "deplie" la structure tf en colonnes
    val avecTemps = jointes
      .withColumn("transaction_time", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("tf", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("tf.*"))
      .drop("tf")

    // 4. Fonctions de fenetrage : une fenetre par utilisateur
    val parUtilisateurOrdonne = Window.partitionBy("user_id").orderBy("transaction_time", "transaction_id")
    val parUtilisateur        = Window.partitionBy("user_id")

    avecTemps
      .withColumn("transaction_rank", row_number().over(parUtilisateurOrdonne))
      .withColumn("user_transaction_count", count(lit(1)).over(parUtilisateur))
      // 5. Tranche d'age
      .withColumn("age_group",
        when(col("age").isNull, "Inconnu")
          .when(col("age") < 25, "Jeune")
          .when(col("age") < 45, "Adulte")
          .when(col("age") < 65, "Âge Moyen")
          .otherwise("Senior"))
  }
}