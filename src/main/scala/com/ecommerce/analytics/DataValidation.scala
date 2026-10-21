package com.ecommerce.ingestion

import com.ecommerce.config.ValidationConfig
import com.ecommerce.models._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object DataValidation {

  def validateTransactions(ds: Dataset[Transaction], cfg: ValidationConfig)(implicit spark: SparkSession): ValidationResult[Transaction] = {
    import spark.implicits._
    val df = ds.toDF()

    val missingIds = col("transaction_id").isNull || trim(col("transaction_id")) === "" ||
                     col("user_id").isNull        || trim(col("user_id")) === "" ||
                     col("product_id").isNull     || trim(col("product_id")) === "" ||
                     col("merchant_id").isNull    || trim(col("merchant_id")) === ""

    val invalidAmount = col("amount").isNull || col("amount") <= cfg.minTransactionAmount
    val invalidTimestamp = col("timestamp").isNull || length(trim(col("timestamp"))) =!= cfg.timestampLength

    val dfWithReason = df.withColumn(
      "rejection_reason",
      concat_ws("; ",
        when(missingIds, lit("MISSING_REQUIRED_ID")),
        when(invalidAmount, lit("INVALID_AMOUNT_LE_ZERO_OR_NULL")),
        when(invalidTimestamp, lit("INVALID_TIMESTAMP_LENGTH_OR_NULL"))
      )
    )

    val validDs = dfWithReason.filter(col("rejection_reason") === "").drop("rejection_reason").as[Transaction]
    val rejectedDf = dfWithReason.filter(col("rejection_reason") =!= "")

    ValidationResult(validDs, rejectedDf)
  }

  def validateUsers(ds: Dataset[User], cfg: ValidationConfig)(implicit spark: SparkSession): ValidationResult[User] = {
    import spark.implicits._
    val df = ds.toDF()

    val missingId = col("user_id").isNull || trim(col("user_id")) === ""
    val invalidAge = col("age").isNull || col("age") < cfg.minUserAge || col("age") > cfg.maxUserAge
    val invalidIncome = col("annual_income").isNull || col("annual_income") <= cfg.minAnnualIncome

    val dfWithReason = df.withColumn(
      "rejection_reason",
      concat_ws("; ",
        when(missingId, lit("MISSING_USER_ID")),
        when(invalidAge, lit(s"INVALID_AGE_OUTSIDE_${cfg.minUserAge}_${cfg.maxUserAge}")),
        when(invalidIncome, lit("INVALID_ANNUAL_INCOME_LE_ZERO_OR_NULL"))
      )
    )

    val validDs = dfWithReason.filter(col("rejection_reason") === "").drop("rejection_reason").as[User]
    val rejectedDf = dfWithReason.filter(col("rejection_reason") =!= "")

    ValidationResult(validDs, rejectedDf)
  }

  def validateProducts(ds: Dataset[ProductItem], cfg: ValidationConfig)(implicit spark: SparkSession): ValidationResult[ProductItem] = {
    import spark.implicits._
    val df = ds.toDF()

    val missingId = col("product_id").isNull || trim(col("product_id")) === ""
    val invalidPrice = col("price").isNull || col("price") <= cfg.minProductPrice
    val invalidRating = col("rating").isNull || col("rating") < cfg.minProductRating || col("rating") > cfg.maxProductRating

    val dfWithReason = df.withColumn(
      "rejection_reason",
      concat_ws("; ",
        when(missingId, lit("MISSING_PRODUCT_ID")),
        when(invalidPrice, lit("INVALID_PRICE_LE_ZERO_OR_NULL")),
        when(invalidRating, lit(s"INVALID_RATING_OUTSIDE_${cfg.minProductRating}_${cfg.maxProductRating}"))
      )
    )

    val validDs = dfWithReason.filter(col("rejection_reason") === "").drop("rejection_reason").as[ProductItem]
    val rejectedDf = dfWithReason.filter(col("rejection_reason") =!= "")

    ValidationResult(validDs, rejectedDf)
  }

  def validateMerchants(ds: Dataset[Merchant], cfg: ValidationConfig)(implicit spark: SparkSession): ValidationResult[Merchant] = {
    import spark.implicits._
    val df = ds.toDF()

    val missingId = col("merchant_id").isNull || trim(col("merchant_id")) === ""
    val invalidCommission = col("commission_rate").isNull || col("commission_rate") < cfg.minCommissionRate || col("commission_rate") > cfg.maxCommissionRate

    val dfWithReason = df.withColumn(
      "rejection_reason",
      concat_ws("; ",
        when(missingId, lit("MISSING_MERCHANT_ID")),
        when(invalidCommission, lit(s"INVALID_COMMISSION_RATE_OUTSIDE_${cfg.minCommissionRate}_${cfg.maxCommissionRate}"))
      )
    )

    val validDs = dfWithReason.filter(col("rejection_reason") === "").drop("rejection_reason").as[Merchant]
    val rejectedDf = dfWithReason.filter(col("rejection_reason") =!= "")

    ValidationResult(validDs, rejectedDf)
  }

  /** Nombre total de valeurs nulles, toutes colonnes confondues. */
  private def countTotalNulls(df: DataFrame): Long = {
    if (df.columns.isEmpty) 0L
    else {
      val nullSumExpr: Column = df.columns
        .map(c => when(col(c).isNull, 1L).otherwise(0L))
        .reduce(_ + _)
      df.select(sum(nullSumExpr)).first().getAs[Long](0)
    }
  }

  /** Calcule les chiffres de qualité d'un dataset et affiche le bilan. */
  def computeMetrics[T](sourceName: String, rawDs: Dataset[T], result: ValidationResult[T]): QualityMetrics = {
    val rawDf     = rawDs.toDF()
    val totalRead = rawDf.count()
    val validCnt  = result.valid.count()
    val rejectCnt = result.rejected.count()
    val rejectPct = if (totalRead > 0) BigDecimal(rejectCnt.toDouble * 100.0 / totalRead).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble else 0.0
    val nullCnt   = countTotalNulls(rawDf)

    println(s"[VALIDATION] $sourceName : lues = $totalRead | valides = $validCnt | rejetees = $rejectCnt | valeurs nulles = $nullCnt")
    QualityMetrics(sourceName, totalRead, validCnt, rejectCnt, rejectPct, nullCnt)
  }

  /** Construit le DataFrame de synthèse du rapport de qualité. */
  def buildQualityReport(metrics: Seq[QualityMetrics])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    metrics.toDF()
  }

  /** Nombre de transactions dont la clé étrangère n'existe pas dans la table de référence. */
  private def countOrphans(tx: DataFrame, reference: DataFrame, key: String): Long =
    tx.select(key)
      .join(reference.select(key).distinct(), Seq(key), "left_anti")
      .count()

  /** Ajoute les compteurs d'orphelins au rapport de qualité. */
  def addReferentialCounters(
    report: DataFrame,
    rawTx: Dataset[Transaction],
    rawUsers: Dataset[User],
    rawProducts: Dataset[ProductItem],
    rawMerchants: Dataset[Merchant]
  ): DataFrame = {
    val tx = rawTx.toDF()
    val u = countOrphans(tx, rawUsers.toDF(), "user_id")
    val p = countOrphans(tx, rawProducts.toDF(), "product_id")
    val m = countOrphans(tx, rawMerchants.toDF(), "merchant_id")
    println(s"[INTEGRITE] user_id orphelins = $u | product_id orphelins = $p | merchant_id orphelins = $m")

    report
      .withColumn("nb_user_id_orphelins", when(col("dataset") === "transactions", lit(u)))
      .withColumn("nb_product_id_orphelins", when(col("dataset") === "transactions", lit(p)))
      .withColumn("nb_merchant_id_orphelins", when(col("dataset") === "transactions", lit(m)))
  }

  /** Enregistre le rapport dans UN SEUL fichier CSV, sans dossier part-xxx. */
  def saveReportAsCsv(report: DataFrame, path: String): Unit = {
    val fichier = Paths.get(path)
    Option(fichier.getParent).foreach(dossier => Files.createDirectories(dossier))
    val entete  = report.columns.mkString(",")
    val lignes  = report.collect().map(_.toSeq.map(v => if (v == null) "" else v.toString).mkString(",")).toList
    val contenu = (entete :: lignes).mkString("\n") + "\n"
    Files.write(fichier, contenu.getBytes(StandardCharsets.UTF_8))
    println(s"[RAPPORT] enregistré dans $fichier")
  }

  /** Vérifie l'intégrité référentielle et retourne les transactions valides vs orphelines. */
  def checkReferentialIntegrity(
    validTx: Dataset[Transaction],
    validUsers: Dataset[User],
    validProducts: Dataset[ProductItem],
    validMerchants: Dataset[Merchant]
  )(implicit spark: SparkSession): ValidationResult[Transaction] = {
    import spark.implicits._
    val txDf = validTx.toDF()
    val userIds     = validUsers.select(col("user_id").as("u_id")).distinct()
    val productIds  = validProducts.select(col("product_id").as("p_id")).distinct()
    val merchantIds = validMerchants.select(col("merchant_id").as("m_id")).distinct()

    val joined = txDf
      .join(userIds, txDf("user_id") === userIds("u_id"), "left")
      .join(productIds, txDf("product_id") === productIds("p_id"), "left")
      .join(merchantIds, txDf("merchant_id") === merchantIds("m_id"), "left")
      .withColumn(
        "rejection_reason",
        concat_ws("; ",
          when(col("u_id").isNull, lit("ORPHAN_USER_ID")),
          when(col("p_id").isNull, lit("ORPHAN_PRODUCT_ID")),
          when(col("m_id").isNull, lit("ORPHAN_MERCHANT_ID"))
        )
      )
      .drop("u_id", "p_id", "m_id")

    val cleanTx = joined.filter(col("rejection_reason") === "").drop("rejection_reason").as[Transaction]
    val orphanTx = joined.filter(col("rejection_reason") =!= "")

    println("\n--- [BONUS] Rapport d'intégrité référentielle ---")
    orphanTx.groupBy("rejection_reason").count().show(truncate = false)

    ValidationResult(cleanTx, orphanTx)
  }
}