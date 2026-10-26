package com.ecommerce

import com.ecommerce.utils.AppConfig
import com.ecommerce.ingestion.{DataIngestion, DataValidation}
import com.ecommerce.analytics.DataTransformation
import com.ecommerce.models._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MainApp {
  def main(args: Array[String]): Unit = {
    val appConfig = AppConfig.load()

    // CONFIGURATION CONDITIONNELLE DE HADOOP (selon la machine)
    if (appConfig.spark.hadoopHomeDir != null && appConfig.spark.hadoopHomeDir.trim.nonEmpty) {
      System.setProperty("hadoop.home.dir", appConfig.spark.hadoopHomeDir)
      println(s"[INFO] Hadoop home configuré dynamiquement à : ${appConfig.spark.hadoopHomeDir}")
    }

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(appConfig.spark.appName)
      .master(appConfig.spark.master)
      .config("spark.sql.shuffle.partitions", appConfig.spark.shufflePartitions)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Récupération de l'argument de ligne de commande (par défaut : "all")
    val mode = if (args.nonEmpty) args(0).toLowerCase else "all"
    println(s"[INFO] Mode d'exécution sélectionné : $mode")

    // Déclaration des variables partagées avec les types forts
    var rawTx: org.apache.spark.sql.Dataset[Transaction] = null
    var rawUsers: org.apache.spark.sql.Dataset[User] = null
    var rawProducts: org.apache.spark.sql.Dataset[ProductItem] = null
    var rawMerchants: org.apache.spark.sql.Dataset[Merchant] = null

    try {
      // ==========================================
      // OPTION 1 : MODULE DATA INGESTION & VALIDATION
      // ==========================================
      if (mode == "all" || mode == "ingestion") {
        println("=== ETAPE 1: Multi-Format Data Ingestion ===")
        rawTx        = DataIngestion.loadTransactions(appConfig.paths.transactions).persist(StorageLevel.MEMORY_AND_DISK_SER)
        rawUsers     = DataIngestion.loadUsers(appConfig.paths.users).cache()
        rawProducts  = DataIngestion.loadProducts(appConfig.paths.products).cache()
        rawMerchants = DataIngestion.loadMerchants(appConfig.paths.merchants).cache()
 
        // --- PREVIEW ROWS ---
        println("\n PREVIEW: transactions.csv (First 5 Rows)")
        rawTx.show(5, truncate = false)

        println("\n PREVIEW: users.json (First 5 Rows)")
        rawUsers.show(5, truncate = false)

        println("\n PREVIEW: products.parquet (First 5 Rows)")
        rawProducts.show(5, truncate = false)

        println("\n PREVIEW: merchants.csv (First 5 Rows)")
        rawMerchants.show(5, truncate = false)

        println("=== ETAPE 2: Data Validation & Quality Report ===")
        val txResult       = DataValidation.validateTransactions(rawTx, appConfig.validation)
        val userResult     = DataValidation.validateUsers(rawUsers, appConfig.validation)
        val productResult  = DataValidation.validateProducts(rawProducts, appConfig.validation)
        val merchantResult = DataValidation.validateMerchants(rawMerchants, appConfig.validation)

        // --- EXPORT DES LIGNES VALIDES ---
        DataValidation.saveValidAsCsv(txResult.valid, "output/valid/transactions_valid.csv", "transactions")
        DataValidation.saveValidAsJson(userResult.valid, "output/valid/users_valid.json", "users")
        DataValidation.saveValidAsParquet(productResult.valid, "output/valid/products_valid.parquet", "products")
        DataValidation.saveValidAsCsv(merchantResult.valid, "output/valid/merchants_valid.csv", "merchants")

        // --- EXPORT DES LIGNES REJETÉES ---
        DataValidation.saveRejectedAsCsv(txResult.rejected, "output/rejected/transactions_rejected.csv", "transactions")
        DataValidation.saveRejectedAsJson(userResult.rejected, "output/rejected/users_rejected.json", "users")
        DataValidation.saveRejectedAsParquet(productResult.rejected, "output/rejected/products_rejected.parquet", "products")
        DataValidation.saveRejectedAsCsv(merchantResult.rejected, "output/rejected/merchants_rejected.csv", "merchants")

        // --- CALCUL DES MÉTRIQUES & RAPPORT ---
        val metrics = Seq(
          DataValidation.computeMetrics("merchants", rawMerchants, merchantResult),
          DataValidation.computeMetrics("products", rawProducts, productResult),
          DataValidation.computeMetrics("transactions", rawTx, txResult, Some(rawTx), Some(rawUsers), Some(rawProducts), Some(rawMerchants)),
          DataValidation.computeMetrics("users", rawUsers, userResult)
        )

        val reportDf = DataValidation.buildQualityReport(metrics)
        DataValidation.saveReportAsCsv(reportDf, appConfig.paths.output.qualityReport)

        val refIntegrityResult = DataValidation.checkReferentialIntegrity(
          txResult.valid, userResult.valid, productResult.valid, merchantResult.valid
        )

        println(s"\n[SUCCESS] Ingestion & Validation terminées : ${refIntegrityResult.valid.count()} rows valides.")
      }

      // Chargement de sécurité mutualisé pour les modules suivants si exécutés isolément
      if (mode == "transformation" || mode == "fraud" || mode == "fraud-detection" || mode == "quality" || mode == "report") {
        if (rawTx == null) {
          rawTx        = DataIngestion.loadTransactions(appConfig.paths.transactions).persist(StorageLevel.MEMORY_AND_DISK_SER)
          rawUsers     = DataIngestion.loadUsers(appConfig.paths.users).cache()
          rawProducts  = DataIngestion.loadProducts(appConfig.paths.products).cache()
          rawMerchants = DataIngestion.loadMerchants(appConfig.paths.merchants).cache()
        }
      }

      val txRes = if (rawTx != null) DataValidation.validateTransactions(rawTx, appConfig.validation) else null
      val usRes = if (rawUsers != null) DataValidation.validateUsers(rawUsers, appConfig.validation) else null
      val prRes = if (rawProducts != null) DataValidation.validateProducts(rawProducts, appConfig.validation) else null
      val meRes = if (rawMerchants != null) DataValidation.validateMerchants(rawMerchants, appConfig.validation) else null

      val transfo = new DataTransformation(spark)

      // ==========================================
      // OPTION 2 : MODULE DATA TRANSFORMATION SEUL
      // ==========================================
      if (mode == "all" || mode == "transformation") {
        println("=== ETAPE 3A: Data Transformation & Enrichment ===")
        val enrichi = transfo.enrichTransactionData(
          txRes.valid.toDF(), usRes.valid.toDF(), prRes.valid.toDF(), meRes.valid.toDF(),
          appConfig.optimizations.enableBroadcast
        )
        println(s"[TRANSFORMATION] Transactions enrichies avec succès : ${enrichi.count()} lignes.")
      }

      // ==========================================
      // OPTION 3 : MODULE FRAUD DETECTION SEUL
      // ==========================================
      if (mode == "all" || mode == "fraud" || mode == "fraud-detection") {
        println("=== ETAPE 3B: Fraud Detection & Analytics ===")
        val enrichi = transfo.enrichTransactionData(
          txRes.valid.toDF(), usRes.valid.toDF(), prRes.valid.toDF(), meRes.valid.toDF(),
          appConfig.optimizations.enableBroadcast
        )
        val complet = transfo.detectSuspicious(transfo.addBehaviorFeatures(enrichi))

        println(s"[FRAUD DETECTION] Transactions totales = ${complet.count()} | Suspectes = ${complet.filter(org.apache.spark.sql.functions.col("is_suspicious") === 1).count()}")
        transfo.showTopSuspicious(complet)
      }

      // ==========================================
      // OPTION 4 : MODULE RAPPORT DE QUALITÉ SEUL
      // ==========================================
      if (mode == "quality" || mode == "report") {
        println("=== ETAPE : Génération du Rapport de Qualité Isolé ===")
        val metrics = Seq(
          DataValidation.computeMetrics("merchants", rawMerchants, meRes),
          DataValidation.computeMetrics("products", rawProducts, prRes),
          DataValidation.computeMetrics("transactions", rawTx, txRes, Some(rawTx), Some(rawUsers), Some(rawProducts), Some(rawMerchants)),
          DataValidation.computeMetrics("users", rawUsers, usRes)
        )

        val reportDf = DataValidation.buildQualityReport(metrics)
        DataValidation.saveReportAsCsv(reportDf, appConfig.paths.output.qualityReport)
        println(s"[SUCCESS] Rapport de qualité généré dans : ${appConfig.paths.output.qualityReport}")
      }

      // --- LIBÉRATION EXPLICITE DU CACHE (UNPERSIST) ---
      if (rawTx != null) rawTx.unpersist()
      if (rawUsers != null) rawUsers.unpersist()
      if (rawProducts != null) rawProducts.unpersist()
      if (rawMerchants != null) rawMerchants.unpersist()

      println(s"\n[SUCCESS] Exécution du module '$mode' terminée avec succès !")

    } catch {
      case e: Exception =>
        System.err.println(s"[ERROR] Execution failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}