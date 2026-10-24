package com.ecommerce.analytics

import com.ecommerce.config.AppConfig
import com.ecommerce.ingestion.{DataIngestion, DataValidation}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** Point d'entree de la partie 3 (Membre B), utilisable seul pour la demonstration.
  * Il utilise l'ingestion et la validation de la partie 2, puis la transformation.
  * Les transactions valides sont gardees meme si leur utilisateur, produit ou marchand
  * est inconnu (jointures left, voir la decision 6 de CONTRIBUTIONS.md).
  */
object TransformationDemo {

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(cfg.spark.appName + " - partie 3")
      .master(cfg.spark.master)
      .config("spark.sql.shuffle.partitions", cfg.spark.shufflePartitions)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val debut = System.nanoTime()

      // Etape 1 : lecture (partie 2)
      val tx = DataIngestion.loadTransactions(cfg.paths.transactions)
      val us = DataIngestion.loadUsers(cfg.paths.users)
      val pr = DataIngestion.loadProducts(cfg.paths.products)
      val me = DataIngestion.loadMerchants(cfg.paths.merchants)

      // Etape 2 : validation (partie 2)
      val rTx = DataValidation.validateTransactions(tx, cfg.validation)
      val rUs = DataValidation.validateUsers(us, cfg.validation)
      val rPr = DataValidation.validateProducts(pr, cfg.validation)
      val rMe = DataValidation.validateMerchants(me, cfg.validation)
      println(s"[VALIDATION] transactions : lues = ${tx.count()} | valides = ${rTx.valid.count()}")

      // Etape 3 : transformation (partie 3)
      val transfo = new DataTransformation(spark)
      val enrichi = transfo.enrichTransactionData(
        rTx.valid.toDF(), rUs.valid.toDF(), rPr.valid.toDF(), rMe.valid.toDF(),
        cfg.optimizations.enableBroadcast)
      val brut = transfo.detectSuspicious(transfo.addBehaviorFeatures(enrichi))
      val complet = if (cfg.optimizations.enableCache) brut.cache() else brut

      println(s"[TRANSFORMATION] transactions enrichies = ${complet.count()} | suspectes = ${complet.filter(col("is_suspicious") === 1).count()}")
      transfo.showTopSuspicious(complet)

      val secondes = (System.nanoTime() - debut) / 1e9
      println(f"[DUREE] ingestion + validation + transformation = $secondes%.2f s")
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
