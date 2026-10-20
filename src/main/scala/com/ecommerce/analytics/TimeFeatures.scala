package com.ecommerce.analytics

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.util.Locale
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** Les caracteristiques temporelles d'un timestamp (une colonne de type struct dans Spark). */
case class TimeFeatures(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

object TimeFeatures {

  // Format yyyyMMddHHmmss, strict : refuse par exemple le mois 13 ou le 31 fevrier
  private val format = DateTimeFormatter
    .ofPattern("uuuuMMddHHmmss")
    .withResolverStyle(ResolverStyle.STRICT)

  /** Fonction pure : renvoie None si le texte est null, vide ou mal forme. */
  def extraire(ts: String): Option[TimeFeatures] = {
    if (ts == null) None
    else {
      val texte = ts.trim
      if (texte.length != 14 || !texte.forall(_.isDigit)) None
      else {
        try {
          val date = LocalDateTime.parse(texte, format)
          val h    = date.getHour
          val jour = date.getDayOfWeek
          Some(TimeFeatures(
            hour             = h,
            day_of_week      = jour.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
            month            = date.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
            is_weekend       = if (jour.getValue >= 6) 1 else 0,
            day_period       = periode(h),
            is_working_hours = if (h >= 9 && h < 17) 1 else 0
          ))
        } catch {
          case _: Exception => None   // date impossible : on renvoie None, pas d'erreur
        }
      }
    }
  }

  /** Morning [6h-12h[, Afternoon [12h-18h[, Evening [18h-22h[, Night sinon. */
  private def periode(h: Int): String =
    if (h >= 6 && h < 12) "Morning"
    else if (h >= 12 && h < 18) "Afternoon"
    else if (h >= 18 && h < 22) "Evening"
    else "Night"

  /** L'UDF Spark : on enveloppe la fonction pure. Un None devient une valeur null. */
  val extractTimeFeatures: UserDefinedFunction = udf((ts: String) => extraire(ts))
}