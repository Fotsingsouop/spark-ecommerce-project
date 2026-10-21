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

  /** Montant sur 7 jours glissants, utilisateur actif, delai depuis la transaction precedente. */
  def addBehaviorFeatures(df: DataFrame): DataFrame = {
    val secondes = col("transaction_time").cast("long")
    val septJours = 7L * 24 * 3600

    // Fenetre glissante : de (t - 7 jours) jusqu'a t, pour un meme utilisateur
    val fenetre7j = Window.partitionBy("user_id").orderBy(secondes)
      .rangeBetween(-septJours, Window.currentRow)
    // Fenetre ordonnee par date, pour retrouver la transaction precedente
    val ordonnee = Window.partitionBy("user_id").orderBy("transaction_time", "transaction_id")

    df.withColumn("date_jour", to_date(col("transaction_time")))
      .withColumn("amount_7d", round(sum("amount").over(fenetre7j), 2))
      .withColumn("active_days_7d", size(collect_set(col("date_jour")).over(fenetre7j)))
      .withColumn("is_active_user", when(col("active_days_7d") >= 5, 1).otherwise(0))
      .withColumn("days_since_previous",
        datediff(col("date_jour"), lag("date_jour", 1).over(ordonnee)))
  }

  /** Ajoute l'ecart au panier moyen, les 4 conditions et le drapeau is_suspicious. */
  def detectSuspicious(df: DataFrame): DataFrame = {
    val parUtilisateur = Window.partitionBy("user_id")
    val ordonnee = Window.partitionBy("user_id").orderBy("transaction_time", "transaction_id")
    val secondes = col("transaction_time").cast("long")

    df.withColumn("avg_user_amount", avg("amount").over(parUtilisateur))
      .withColumn("basket_gap_pct",
        round((col("amount") - col("avg_user_amount")) * 100 / col("avg_user_amount"), 2))
      .withColumn("seconds_since_previous", secondes - lag(secondes, 1).over(ordonnee))
      .withColumn("cond_high_amount", when(col("amount") > col("avg_user_amount") * 4, 1).otherwise(0))
      .withColumn("cond_night", when(col("day_period") === "Night", 1).otherwise(0))
      .withColumn("cond_fast", when(col("seconds_since_previous") < 300, 1).otherwise(0))
      .withColumn("cond_crypto", when(col("payment_method") === "CRYPTO", 1).otherwise(0))
      .withColumn("is_suspicious",
        when(col("cond_high_amount") + col("cond_night") + col("cond_fast") + col("cond_crypto") >= 2, 1)
          .otherwise(0))
  }
 
}