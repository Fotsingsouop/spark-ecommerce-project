package com.ecommerce

import com.ecommerce.config.AppConfig
import com.ecommerce.ingestion.{DataIngestion, DataValidation}
import org.apache.spark.sql.SparkSession


object MainApp {
  def main(args: Array[String]): Unit = {
    val appConfig = AppConfig.load()

//  FORCE HADOOP TO LOAD LOCAL BINARIES PROGRAMMATICALLY
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
 
// --- ADD THESE PREVIEW ROWS ---
      println("\n PREVIEW: transactions.csv (First 5 Rows)")
      rawTx.show(5, truncate = false)

      println("\n PREVIEW: users.json (First 5 Rows)")
      rawUsers.show(5, truncate = false)

      println("\n PREVIEW: products.parquet (First 5 Rows)")
      rawProducts.show(5, truncate = false)

      println("\n PREVIEW: merchants.csv (First 5 Rows)")
      rawMerchants.show(5, truncate = false)
      // ------------------------------

      println("=== STEP 2: Data Validation & Quality Report ===")
      val txResult       = DataValidation.validateTransactions(rawTx, appConfig.validation)
      val userResult     = DataValidation.validateUsers(rawUsers, appConfig.validation)
      val productResult  = DataValidation.validateProducts(rawProducts, appConfig.validation)
      val merchantResult = DataValidation.validateMerchants(rawMerchants, appConfig.validation)

      val metrics = Seq(
        DataValidation.computeMetrics("transactions", rawTx, txResult),
        DataValidation.computeMetrics("users", rawUsers, userResult),
        DataValidation.computeMetrics("products", rawProducts, productResult),
        DataValidation.computeMetrics("merchants", rawMerchants, merchantResult)
      )

      DataValidation.generateQualityReport(metrics, appConfig.paths.output.qualityReport)

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
