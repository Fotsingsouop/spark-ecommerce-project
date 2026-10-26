package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import com.ecommerce.ingestion.{DataIngestion, DataValidation}
import org.apache.spark.sql.SparkSession

/** Point d'entree de la partie 4, utilisable seul pour tester Analytics et pour la demonstration.
  * Il reprend l'ingestion et la validation (partie 2), la transformation (partie 3),
  * puis appelle Analytics (partie 4).
  */
object AnalyticsDemo {

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(cfg.spark.appName + " - partie 4")
      .master(cfg.spark.master)
      .config("spark.sql.shuffle.partitions", cfg.spark.shufflePartitions)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val debut = System.nanoTime()

      // Parties 2 et 3 : lecture, validation, enrichissement, detection des suspects
      val tx = DataIngestion.loadTransactions(cfg.paths.transactions)
      val us = DataIngestion.loadUsers(cfg.paths.users)
      val pr = DataIngestion.loadProducts(cfg.paths.products)
      val me = DataIngestion.loadMerchants(cfg.paths.merchants)

      val rTx = DataValidation.validateTransactions(tx, cfg.validation)
      val rUs = DataValidation.validateUsers(us, cfg.validation)
      val rPr = DataValidation.validateProducts(pr, cfg.validation)
      val rMe = DataValidation.validateMerchants(me, cfg.validation)

      val transfo = new DataTransformation(spark)
      val enrichi = transfo.enrichTransactionData(
        rTx.valid.toDF(), rUs.valid.toDF(), rPr.valid.toDF(), rMe.valid.toDF(),
        cfg.optimizations.enableBroadcast)
      val brut = transfo.detectSuspicious(transfo.addBehaviorFeatures(enrichi))
      val complet = if (cfg.optimizations.enableCache) brut.cache() else brut

      // Partie 4
      val analytics = new Analytics(spark)

      val rapportMarchands = analytics.merchantReport(complet)
      println(s"[ANALYTICS] marchands dans le rapport = ${rapportMarchands.count()}")
      analytics.showMerchantReport(rapportMarchands)

      val retention = analytics.cohortAnalysis(complet)
      println(s"[ANALYTICS] lignes de la matrice de retention = ${retention.count()}")
      analytics.showCohorts(retention)

      val secondes = (System.nanoTime() - debut) / 1e9
      println(f"[DUREE] ingestion + validation + transformation + analytique = $secondes%.2f s")
      complet.unpersist()
    } catch {
      case e: Exception =>
        println(s"[ERREUR] ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
