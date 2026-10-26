package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory

/** Seuils de la partie 3. Ils sont lus dans application.conf (bloc app.transformation).
  * Si une cle est absente, la valeur par defaut ci-dessous est utilisee.
  */
case class SeuilsTransformation(
  multiplicateurMontant: Double,   // montant suspect si > multiplicateur x panier moyen de l'utilisateur
  delaiRapideSecondes: Long,       // achat "rapide" si moins de N secondes apres le precedent
  nbConditionsMin: Int,            // transaction suspecte si au moins N conditions sur 4
  joursFenetre: Int,               // taille de la fenetre glissante, en jours
  joursActifsMin: Int)             // utilisateur actif si au moins N jours distincts dans la fenetre

object SeuilsTransformation {
  val defaut: SeuilsTransformation = SeuilsTransformation(4.0, 300L, 2, 7, 5)

  def charger(): SeuilsTransformation = {
    val conf = ConfigFactory.load()
    val racine = "app.transformation"
    def lire[T](cle: String, valeurParDefaut: T)(lecture: String => T): T =
      if (conf.hasPath(s"$racine.$cle")) lecture(s"$racine.$cle") else valeurParDefaut

    SeuilsTransformation(
      lire("amount-multiplier", defaut.multiplicateurMontant)(conf.getDouble),
      lire("fast-purchase-seconds", defaut.delaiRapideSecondes)(conf.getLong),
      lire("min-conditions", defaut.nbConditionsMin)(conf.getInt),
      lire("window-days", defaut.joursFenetre)(conf.getInt),
      lire("active-days-min", defaut.joursActifsMin)(conf.getInt))
  }
}

class DataTransformation(spark: SparkSession, seuils: SeuilsTransformation = SeuilsTransformation.charger()) {

  /** Hint broadcast sur une petite table de reference, si l'option est activee. */
  private def broadcastIf(df: DataFrame, actif: Boolean): DataFrame =
    if (actif) broadcast(df) else df

  /** Joint les 4 tables, ajoute les features temporelles, le rang, le total et la tranche d'age. */
    def enrichTransactionData(transactions: DataFrame, users: DataFrame,
                            products: DataFrame, merchants: DataFrame,
                            useBroadcast: Boolean = false): DataFrame = {

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
      .join(broadcastIf(u, useBroadcast), Seq("user_id"), "left")
      .join(broadcastIf(p, useBroadcast), Seq("product_id"), "left")
      .join(broadcastIf(m, useBroadcast), Seq("merchant_id"), "left")

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
    val septJours = seuils.joursFenetre.toLong * 24 * 3600

    // Fenetre glissante : de (t - 7 jours) jusqu'a t, pour un meme utilisateur
    val fenetre7j = Window.partitionBy("user_id").orderBy(secondes)
      .rangeBetween(-septJours, Window.currentRow)
    // Fenetre ordonnee par date, pour retrouver la transaction precedente
    val ordonnee = Window.partitionBy("user_id").orderBy("transaction_time", "transaction_id")

    df.withColumn("date_jour", to_date(col("transaction_time")))
      .withColumn("amount_7d", round(sum("amount").over(fenetre7j), 2))
      .withColumn("active_days_7d", size(collect_set(col("date_jour")).over(fenetre7j)))
      .withColumn("is_active_user", when(col("active_days_7d") >= seuils.joursActifsMin, 1).otherwise(0))
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
      .withColumn("cond_high_amount", when(col("amount") > col("avg_user_amount") * seuils.multiplicateurMontant, 1).otherwise(0))
      .withColumn("cond_night", when(col("day_period") === "Night", 1).otherwise(0))
      .withColumn("cond_fast", when(col("seconds_since_previous") < seuils.delaiRapideSecondes, 1).otherwise(0))
      .withColumn("cond_crypto", when(col("payment_method") === "CRYPTO", 1).otherwise(0))
      .withColumn("nb_conditions",
        col("cond_high_amount") + col("cond_night") + col("cond_fast") + col("cond_crypto"))
      .withColumn("suspicion_raisons",
        concat_ws("+",
          when(col("cond_high_amount") === 1, lit("MONTANT_ELEVE")),
          when(col("cond_night") === 1, lit("NUIT")),
          when(col("cond_fast") === 1, lit("RAPIDE")),
          when(col("cond_crypto") === 1, lit("CRYPTO"))))
      .withColumn("is_suspicious", when(col("nb_conditions") >= seuils.nbConditionsMin, 1).otherwise(0))
  }

  /** Affiche les 20 transactions suspectes avec les montants les plus eleves et les raisons du signalement.
    * Une transaction est suspecte avec au moins 2 conditions sur 4 : le mode de paiement CRYPTO n'est donc
    * qu'une des raisons possibles. Pour la lisibilite, une categorie de marchand absente est affichee "INCONNU"
    * (affichage seulement : les donnees enrichies gardent la valeur null).
    */
  def showTopSuspicious(df: DataFrame): Unit = {
    println("=== TOP 20 DES TRANSACTIONS SUSPECTES (montants les plus élevés)")
    df.filter(col("is_suspicious") === 1)
      .orderBy(col("amount").desc)
      .select(col("transaction_id"), col("user_id"), col("amount"), col("day_period"), col("payment_method"),
        coalesce(col("merchant_category"), lit("INCONNU")).as("merchant_category"),
        col("nb_conditions"), col("suspicion_raisons"))
      .show(20, false)
  }
 
}