package com.ecommerce

import com.ecommerce.config.AppConfig
import com.ecommerce.ingestion.{DataIngestion, DataValidation}
import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {
    val appConfig = AppConfig.load()

    // FORCE HADOOP TO LOAD LOCAL BINARIES PROGRAMMATICALLY
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(appConfig.spark.appName)
      .master(appConfig.spark.master)
      .config("spark.sql.shuffle.partitions", appConfig.spark.shufflePartitions)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      println("=== STEP 1: Multi-Format Data Ingestion ===")
      val rawTx        = DataIngestion.loadTransactions(appConfig.paths.transactions)
      val rawUsers     = DataIngestion.loadUsers(appConfig.paths.users)
      val rawProducts  = DataIngestion.loadProducts(appConfig.paths.products)
      val rawMerchants = DataIngestion.loadMerchants(appConfig.paths.merchants)
 
      // --- PREVIEW ROWS ---
      println("\n PREVIEW: transactions.csv (First 5 Rows)")
      rawTx.show(5, truncate = false)

      println("\n PREVIEW: users.json (First 5 Rows)")
      rawUsers.show(5, truncate = false)

      println("\n PREVIEW: products.parquet (First 5 Rows)")
      rawProducts.show(5, truncate = false)

      println("\n PREVIEW: merchants.csv (First 5 Rows)")
      rawMerchants.show(5, truncate = false)
      // --------------------

      println("=== STEP 2: Data Validation & Quality Report ===")
      val txResult       = DataValidation.validateTransactions(rawTx, appConfig.validation)
      val userResult     = DataValidation.validateUsers(rawUsers, appConfig.validation)
      val productResult  = DataValidation.validateProducts(rawProducts, appConfig.validation)
      val merchantResult = DataValidation.validateMerchants(rawMerchants, appConfig.validation)

      // --- EXPORT DES LIGNES VALIDES (RESPECTANT LE FORMAT D'ORIGINE) ---
      DataValidation.saveValidAsCsv(txResult.valid, "output/valid/transactions_valid.csv", "transactions")             // CSV
      DataValidation.saveValidAsJson(userResult.valid, "output/valid/users_valid.json", "users")                       // JSON
      DataValidation.saveValidAsParquet(productResult.valid, "output/valid/products_valid.parquet", "products")       // PARQUET
      DataValidation.saveValidAsCsv(merchantResult.valid, "output/valid/merchants_valid.csv", "merchants")             // CSV

      // --- EXPORT DES LIGNES REJETÉES (RESPECTANT LE FORMAT D'ORIGINE) ---
      DataValidation.saveRejectedAsCsv(txResult.rejected, "output/rejected/transactions_rejected.csv", "transactions") // CSV
      DataValidation.saveRejectedAsJson(userResult.rejected, "output/rejected/users_rejected.json", "users")           // JSON
      DataValidation.saveRejectedAsParquet(productResult.rejected, "output/rejected/products_rejected.parquet", "products") // PARQUET
      DataValidation.saveRejectedAsCsv(merchantResult.rejected, "output/rejected/merchants_rejected.csv", "merchants") // CSV

      // --- CALCUL DES MÉTRIQUES (avec gestion des orphelins) ---
      val metrics = Seq(
        DataValidation.computeMetrics("merchants", rawMerchants, merchantResult),
        DataValidation.computeMetrics("products", rawProducts, productResult),
        DataValidation.computeMetrics("transactions", rawTx, txResult, Some(rawTx), Some(rawUsers), Some(rawProducts), Some(rawMerchants)),
        DataValidation.computeMetrics("users", rawUsers, userResult)
      )

      // --- GÉNÉRATION ET ENREGISTREMENT DU RAPPORT DE QUALITÉ ---
      val reportDf = DataValidation.buildQualityReport(metrics)
      DataValidation.saveReportAsCsv(reportDf, appConfig.paths.output.qualityReport)

      // Bonus 2.5: Referential Integrity Check
      val refIntegrityResult = DataValidation.checkReferentialIntegrity(
        txResult.valid,
        userResult.valid,
        productResult.valid,
        merchantResult.valid
      )

      println(s"\n[SUCCESS] Final clean transactions ready for Member B: ${refIntegrityResult.valid.count()} rows")

    } catch {
      case e: Exception =>
        System.err.println(s"[ERROR] Execution failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}