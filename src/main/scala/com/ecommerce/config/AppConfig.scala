package com.ecommerce.config

import com.typesafe.config.{Config, ConfigFactory}

case class SparkConfig(appName: String, master: String, shufflePartitions: Int)
case class OutputPaths(baseDir: String, qualityReport: String, rejectedRecords: String, enrichedData: String, merchantKpis: String, cohortRetention: String)
case class DataPaths(transactions: String, users: String, products: String, merchants: String, output: OutputPaths)
case class OptimizationConfig(enableCache: Boolean, enableBroadcast: Boolean)
case class ValidationConfig(
  minTransactionAmount: Double,
  timestampLength: Int,
  minUserAge: Int,
  maxUserAge: Int,
  minAnnualIncome: Double,
  minProductPrice: Double,
  minProductRating: Double,
  maxProductRating: Double,
  minCommissionRate: Double,
  maxCommissionRate: Double
)

case class AppConfig(
  spark: SparkConfig,
  paths: DataPaths,
  optimizations: OptimizationConfig,
  validation: ValidationConfig
)

object AppConfig {
  def load(): AppConfig = {
    val conf: Config = ConfigFactory.load().getConfig("app")
    AppConfig(
      spark = SparkConfig(
        conf.getString("spark.app-name"),
        conf.getString("spark.master"),
        conf.getInt("spark.shuffle-partitions")
      ),
      paths = DataPaths(
        conf.getString("paths.transactions"),
        conf.getString("paths.users"),
        conf.getString("paths.products"),
        conf.getString("paths.merchants"),
        OutputPaths(
          conf.getString("paths.output.base-dir"),
          conf.getString("paths.output.quality-report"),
          conf.getString("paths.output.rejected-records"),
          conf.getString("paths.output.enriched-data"),
          conf.getString("paths.output.merchant-kpis"),
          conf.getString("paths.output.cohort-retention")
        )
      ),
      optimizations = OptimizationConfig(
        conf.getBoolean("optimizations.enable-cache"),
        conf.getBoolean("optimizations.enable-broadcast")
      ),
      validation = ValidationConfig(
        conf.getDouble("validation.min-transaction-amount"),
        conf.getInt("validation.timestamp-length"),
        conf.getInt("validation.min-user-age"),
        conf.getInt("validation.max-user-age"),
        conf.getDouble("validation.min-annual-income"),
        conf.getDouble("validation.min-product-price"),
        conf.getDouble("validation.min-product-rating"),
        conf.getDouble("validation.max-product-rating"),
        conf.getDouble("validation.min-commission-rate"),
        conf.getdouble("validation.max-commission-rate")
      )
    )
  }
}
