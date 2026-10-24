package com.ecommerce.ingestion

import com.ecommerce.utils.ValidationConfig
import com.ecommerce.models._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, SparkSession}
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

  // --- EXPORTS FORMAT CSV (Transactions & Merchants) ---
  def saveValidAsCsv[T](validDs: Dataset[T], path: String, datasetName: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val validDf = validDs.toDF()
    println(s"\n[VALIDES] --- APERÇU (5 premières lignes propres : $datasetName) ---")
    validDf.show(5, truncate = false)

    val fichier = Paths.get(path)
    Option(fichier.getParent).foreach(Files.createDirectories(_))
    val entete = validDf.columns.mkString(",")
    val lignes = validDf.collect().map(_.toSeq.map(v => if (v == null) "" else v.toString).mkString(",")).toList
    Files.write(fichier, (entete :: lignes).mkString("\n").getBytes(StandardCharsets.UTF_8))
    println(s"[VALIDES] Export CSV enregistré dans $fichier")
  }

  def saveRejectedAsCsv(rejectedDf: DataFrame, path: String, datasetName: String): Unit = {
    println(s"\n[REJET] --- APERÇU (5 premières lignes rejetées : $datasetName) ---")
    rejectedDf.show(5, truncate = false)

    val fichier = Paths.get(path)
    Option(fichier.getParent).foreach(Files.createDirectories(_))
    val entete = rejectedDf.columns.mkString(",")
    val lignes = rejectedDf.collect().map(_.toSeq.map(v => if (v == null) "" else v.toString).mkString(",")).toList
    Files.write(fichier, (entete :: lignes).mkString("\n").getBytes(StandardCharsets.UTF_8))
    println(s"[REJET] Export CSV enregistré dans $fichier")
  }

  // --- EXPORTS FORMAT JSON (Users) ---

  def saveValidAsJson[T](validDs: Dataset[T], path: String, datasetName: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val validDf = validDs.toDF()
    println(s"\n[VALIDES] --- APERÇU (5 premières lignes propres : $datasetName) ---")
    validDf.show(5, truncate = false)

    val jsonLines = validDs.toJSON.collect().toList
    val contenu = jsonLines.mkString("\n") + (if (jsonLines.nonEmpty) "\n" else "")

    val fichier = Paths.get(path)
    Option(fichier.getParent).foreach(Files.createDirectories(_))
    Files.write(fichier, contenu.getBytes(StandardCharsets.UTF_8))
    println(s"[VALIDES] Export JSON propre enregistré dans $fichier")
  }

  def saveRejectedAsJson(rejectedDf: DataFrame, path: String, datasetName: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    println(s"\n[REJET] --- APERÇU (5 premières lignes rejetées : $datasetName) ---")
    rejectedDf.show(5, truncate = false)

    val jsonLines = rejectedDf.toJSON.collect().toList
    val contenu = jsonLines.mkString("\n") + (if (jsonLines.nonEmpty) "\n" else "")

    val fichier = Paths.get(path)
    Option(fichier.getParent).foreach(Files.createDirectories(_))
    Files.write(fichier, contenu.getBytes(StandardCharsets.UTF_8))
    println(s"[REJET] Export JSON rejeté enregistré dans $fichier")
  }
  // --- EXPORTS FORMAT PARQUET (Products) ---
  def saveValidAsParquet[T](validDs: Dataset[T], path: String, datasetName: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val validDf = validDs.toDF()
    println(s"\n[VALIDES] --- APERÇU (5 premières lignes propres : $datasetName) ---")
    validDf.show(5, truncate = false)

    validDf.write.mode(SaveMode.Overwrite).parquet(path)
    println(s"[VALIDES] Export Parquet enregistré dans $path")
  }

  def saveRejectedAsParquet(rejectedDf: DataFrame, path: String, datasetName: String): Unit = {
    println(s"\n[REJET] --- APERÇU (5 premières lignes rejetées : $datasetName) ---")
    rejectedDf.show(5, truncate = false)

    rejectedDf.write.mode(SaveMode.Overwrite).parquet(path)
    println(s"[REJET] Export Parquet enregistré dans $path")
  }

  private def countTotalNulls(df: DataFrame): Long = {
    if (df.columns.isEmpty) 0L
    else {
      val nullSumExpr: Column = df.columns
        .map(c => when(col(c).isNull, 1L).otherwise(0L))
        .reduce(_ + _)
      df.select(sum(nullSumExpr)).first().getAs[Long](0)
    }
  }

  private def countOrphans(tx: DataFrame, reference: DataFrame, key: String): Long =
    tx.select(key)
      .join(reference.select(key).distinct(), Seq(key), "left_anti")
      .count()

  def computeMetrics[T](
    sourceName: String, 
    rawDs: Dataset[T], 
    result: ValidationResult[T],
    rawTx: Option[Dataset[Transaction]] = None,
    rawUsers: Option[Dataset[User]] = None,
    rawProducts: Option[Dataset[ProductItem]] = None,
    rawMerchants: Option[Dataset[Merchant]] = None
  ): QualityMetrics = {
    val rawDf     = rawDs.toDF()
    val totalRead = rawDf.count()
    val validCnt  = result.valid.count()
    val rejectCnt = result.rejected.count()
    val rejectPct = if (totalRead > 0) BigDecimal(rejectCnt.toDouble * 100.0 / totalRead).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble else 0.0
    val nullCnt   = countTotalNulls(rawDf)

    val (uOrphans, pOrphans, mOrphans) = if (sourceName == "transactions") {
      val txDf = rawDf
      val u = rawUsers.map(u => countOrphans(txDf, u.toDF(), "user_id")).getOrElse(0L)
      val p = rawProducts.map(p => countOrphans(txDf, p.toDF(), "product_id")).getOrElse(0L)
      val m = rawMerchants.map(m => countOrphans(txDf, m.toDF(), "merchant_id")).getOrElse(0L)
      (java.lang.Long.valueOf(u), java.lang.Long.valueOf(p), java.lang.Long.valueOf(m))
    } else {
      (null.asInstanceOf[java.lang.Long], null.asInstanceOf[java.lang.Long], null.asInstanceOf[java.lang.Long])
    }

    println(s"[VALIDATION] $sourceName : lues = $totalRead | valides = $validCnt | rejetees = $rejectCnt | valeurs nulles = $nullCnt")
    QualityMetrics(sourceName, totalRead, validCnt, rejectCnt, rejectPct, nullCnt, uOrphans, pOrphans, mOrphans)
  }

  def buildQualityReport(metrics: Seq[QualityMetrics])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val reportDf = metrics.toDF()
    reportDf.show(truncate = false)
    reportDf
  }

  def saveReportAsCsv(report: DataFrame, path: String): Unit = {
    val fichier = Paths.get(path)
    Option(fichier.getParent).foreach(dossier => Files.createDirectories(dossier))
    val entete  = report.columns.mkString(",")
    val lignes  = report.collect().map(_.toSeq.map(v => if (v == null) "NULL" else v.toString).mkString(",")).toList
    val contenu = (entete :: lignes).mkString("\n") + "\n"
    Files.write(fichier, contenu.getBytes(StandardCharsets.UTF_8))
    println(s"[RAPPORT] enregistré dans $fichier")
  }

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

    println("\n--- [Question 2.5 BONUS] Rapport d'intégrité référentielle ---")
    orphanTx.groupBy("rejection_reason").count().show(truncate = false)

    ValidationResult(cleanTx, orphanTx)
  }
}