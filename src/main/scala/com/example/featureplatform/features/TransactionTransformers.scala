package com.example.featureplatform.features

import io.circe.Json
import io.circe.syntax._ // For .as[T] syntax
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, functions => F}
import org.slf4j.LoggerFactory

/**
 * Utility object for safely extracting typed parameters from a `Map[String, Json]`.
 * Used by feature transformers to parse their configurations.
 */
object TransformerUtils {
  /** Extracts a String parameter, providing a default value if missing or wrong type. */
  def getStringParam(params: Map[String, Json], key: String, defaultValue: String): String = {
    params.get(key).flatMap(_.asString).getOrElse(defaultValue)
  }

  /** Extracts an optional String parameter. */
  def getOptionalStringParam(params: Map[String, Json], key: String): Option[String] = {
    params.get(key).flatMap(_.asString)
  }

  /** Extracts a Double parameter, providing a default value if missing or wrong type. */  
  def getDoubleParam(params: Map[String, Json], key: String, defaultValue: Double): Double = {
    params.get(key).flatMap(_.as[Double].toOption).getOrElse(defaultValue)
  }

  /** Extracts a List of Strings parameter, providing a default value if missing or wrong type. */
  def getStringListParam(params: Map[String, Json], key: String, defaultValue: List[String]): List[String] = {
    params.get(key).flatMap(_.as[List[String]].toOption).getOrElse(defaultValue)
  }
}

/**
 * Derives boolean indicators `is_credit` and `is_debit` based on a credit/debit indicator column.
 * @param params Configuration parameters:
 *               `credit_debit_indicator_col` (String, default: "creditdebitindicator"): Input column name.
 *               `output_col_credit` (String, default: "is_credit"): Output column for credit flag.
 *               `output_col_debit` (String, default: "is_debit"): Output column for debit flag.
 */
class TransactionIndicatorDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val creditDebitIndicatorCol: String = TransformerUtils.getStringParam(params, "credit_debit_indicator_col", "creditdebitindicator")
  private val outputColCredit: String = TransformerUtils.getStringParam(params, "output_col_credit", "is_credit")
  private val outputColDebit: String = TransformerUtils.getStringParam(params, "output_col_debit", "is_debit")

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionIndicatorDeriver. Input col: $creditDebitIndicatorCol, Output credit: $outputColCredit, Output debit: $outputColDebit")
    df.withColumn(outputColCredit, F.upper(F.col(creditDebitIndicatorCol)) === "CREDIT")
      .withColumn(outputColDebit, F.upper(F.col(creditDebitIndicatorCol)) === "DEBIT")
  }
}

/**
 * Derives hour of day and day of week from a transaction timestamp column.
 * @param params Configuration parameters:
 *               `transaction_timestamp_col` (String, default: "transactiontimestamp"): Input timestamp column.
 *               `output_col_hour` (String, default: "transaction_hour"): Output column for hour.
 *               `output_col_day_of_week` (String, default: "transaction_day_of_week"): Output column for day of week.
 */
class TransactionDatetimeDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionTimestampCol: String = TransformerUtils.getStringParam(params, "transaction_timestamp_col", "transactiontimestamp")
  private val outputColHour: String = TransformerUtils.getStringParam(params, "output_col_hour", "transaction_hour")
  private val outputColDayOfWeek: String = TransformerUtils.getStringParam(params, "output_col_day_of_week", "transaction_day_of_week")

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionDatetimeDeriver. Input col: $transactionTimestampCol")
    df.withColumn(outputColHour, F.hour(F.col(transactionTimestampCol).cast(TimestampType)))
      .withColumn(outputColDayOfWeek, F.dayofweek(F.col(transactionTimestampCol).cast(TimestampType)))
  }
}

/**
 * Creates one-hot encoded columns for transaction statuses.
 * @param params Configuration parameters:
 *               `transaction_status_col` (String, default: "transactionstatus"): Input status column.
 *               `output_col_prefix` (String, default: "transaction_status"): Prefix for new status columns.
 *               `status_values` (List[String], default: List("SUCCESS", "FAILURE", "PENDING")): Statuses to encode.
 */
class TransactionStatusDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionStatusCol: String = TransformerUtils.getStringParam(params, "transaction_status_col", "transactionstatus")
  private val outputColPrefix: String = TransformerUtils.getStringParam(params, "output_col_prefix", "transaction_status")
  private val statusValues: List[String] = TransformerUtils.getStringListParam(params, "status_values", List("SUCCESS", "FAILURE", "PENDING"))


  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionStatusDeriver. Input col: $transactionStatusCol, Prefix: $outputColPrefix")
    var tempDf = df
    statusValues.foreach { status =>
      // Ensure generated column name is valid (e.g. uppercase, no spaces)
      tempDf = tempDf.withColumn(s"${outputColPrefix}_${status.toUpperCase.replaceAll("[^a-zA-Z0-9_]", "")}", F.upper(F.col(transactionStatusCol)) === status.toUpperCase)
    }
    tempDf
  }
}

/**
 * Creates one-hot encoded columns for transaction channels.
 * @param params Configuration parameters:
 *               `transaction_channel_col` (String, default: "transactionchannel"): Input channel column.
 *               `output_col_prefix` (String, default: "transaction_channel"): Prefix for new channel columns.
 *               `channel_values` (List[String], default: List("BRANCH", "ATM", "POS", "INTERNET_BANKING", "MOBILE_BANKING")): Channels to encode.
 */
class TransactionChannelDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionChannelCol: String = TransformerUtils.getStringParam(params, "transaction_channel_col", "transactionchannel")
  private val outputColPrefix: String = TransformerUtils.getStringParam(params, "output_col_prefix", "transaction_channel")
  private val channelValues: List[String] = TransformerUtils.getStringListParam(params, "channel_values", List("BRANCH", "ATM", "POS", "INTERNET_BANKING", "MOBILE_BANKING"))

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionChannelDeriver. Input col: $transactionChannelCol, Prefix: $outputColPrefix")
    var tempDf = df
    channelValues.foreach { channel =>
      tempDf = tempDf.withColumn(s"${outputColPrefix}_${channel.toUpperCase.replaceAll("[^a-zA-Z0-9_]", "")}", F.upper(F.col(transactionChannelCol)) === channel.toUpperCase)
    }
    tempDf
  }
}

/**
 * Derives a boolean flag indicating if a transaction value is above a certain threshold.
 * @param params Configuration parameters:
 *               `input_col` (String, default: "transactionamount"): Input transaction amount column.
 *               `output_col` (String, default: "is_high_value_transaction"): Output boolean column.
 *               `high_value_threshold` (Double, default: 1000.0): Threshold to determine high value.
 */
class TransactionValueDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionAmountCol: String = TransformerUtils.getStringParam(params, "input_col", "transactionamount")
  private val outputColIsHighValue: String = TransformerUtils.getStringParam(params, "output_col", "is_high_value_transaction")
  private val highValueThreshold: Double = TransformerUtils.getDoubleParam(params, "high_value_threshold", 1000.0)

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionValueDeriver. Input col: $transactionAmountCol, Threshold: $highValueThreshold")
    df.withColumn(outputColIsHighValue, F.col(transactionAmountCol) > highValueThreshold)
  }
}

/**
 * Creates one-hot encoded columns for transaction modes.
 * @param params Configuration parameters:
 *               `transaction_mode_col` (String, default: "transactionmode"): Input mode column.
 *               `output_col_prefix` (String, default: "transaction_mode"): Prefix for new mode columns.
 *               `mode_values` (List[String], default: List("CARD", "CASH", "TRANSFER", "UPI", "CHEQUE")): Modes to encode.
 */
class TransactionModeDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val transactionModeCol: String = TransformerUtils.getStringParam(params, "transaction_mode_col", "transactionmode")
  private val outputColPrefix: String = TransformerUtils.getStringParam(params, "output_col_prefix", "transaction_mode")
  private val modeValues: List[String] = TransformerUtils.getStringListParam(params, "mode_values", List("CARD", "CASH", "TRANSFER", "UPI", "CHEQUE"))

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionModeDeriver. Input col: $transactionModeCol, Prefix: $outputColPrefix")
    var tempDf = df
    modeValues.foreach { mode =>
      tempDf = tempDf.withColumn(s"${outputColPrefix}_${mode.toUpperCase.replaceAll("[^a-zA-Z0-9_]", "")}", F.upper(F.col(transactionModeCol)) === mode.toUpperCase)
    }
    tempDf
  }
}

/**
 * Creates one-hot encoded columns for merchant categories.
 * @param params Configuration parameters:
 *               `merchant_category_col` (String, default: "merchantcategory"): Input category column.
 *               `output_col_prefix` (String, default: "transaction_category"): Prefix for new category columns.
 *               `category_values` (List[String], default: List("GROCERIES", "UTILITIES", "ENTERTAINMENT", "TRAVEL", "HEALTHCARE")): Categories to encode.
 */
class TransactionCategoryDeriver(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val merchantCategoryCol: String = TransformerUtils.getStringParam(params, "merchant_category_col", "merchantcategory")
  private val outputColPrefix: String = TransformerUtils.getStringParam(params, "output_col_prefix", "transaction_category")
  private val categoryValues: List[String] = TransformerUtils.getStringListParam(params, "category_values", List("GROCERIES", "UTILITIES", "ENTERTAINMENT", "TRAVEL", "HEALTHCARE"))

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying TransactionCategoryDeriver. Input col: $merchantCategoryCol, Prefix: $outputColPrefix")
    var tempDf = df
    categoryValues.foreach { category =>
      tempDf = tempDf.withColumn(s"${outputColPrefix}_${category.toUpperCase.replaceAll("[^a-zA-Z0-9_]", "")}", F.upper(F.col(merchantCategoryCol)) === category.toUpperCase)
    }
    tempDf
  }
}

/**
 * Aggregates user spending (sum, average, count) over a specified time window.
 * @param params Configuration parameters:
 *               `user_id_col` (String, default: "userid"): User identifier column.
 *               `transaction_amount_col` (String, default: "transactionamount"): Transaction amount column.
 *               `transaction_date_col` (String, default: "transactiondate"): Transaction date/timestamp column.
 *               `window_days` (Int, default: 30): Number of days for the rolling window.
 *               `output_col_prefix` (String, default: "user_spend"): Prefix for new aggregated columns.
 */
class UserSpendAggregator(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val userIdCol: String = TransformerUtils.getStringParam(params, "user_id_col", "userid")
  val transactionAmountCol: String = TransformerUtils.getStringParam(params, "transaction_amount_col", "transactionamount")
  val transactionDateCol: String = TransformerUtils.getStringParam(params, "transaction_date_col", "transactiondate") // Python used transactiondate
  val windowDays: Int = params.get("window_days").flatMap(_.as[Int].toOption).getOrElse(30)
  val outputColPrefix: String = TransformerUtils.getStringParam(params, "output_col_prefix", "user_spend")

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying UserSpendAggregator. User ID: $userIdCol, Amount: $transactionAmountCol, Date: $transactionDateCol, Window: $windowDays days")
    val windowSpec = Window.partitionBy(F.col(userIdCol))
                           .orderBy(F.col(transactionDateCol).cast(TimestampType).cast("long")) // Ensure correct type for ordering
                           .rangeBetween(-windowDays * 24L * 60L * 60L, 0L) // seconds in windowDays (use Long for clarity)

    df.withColumn(s"${outputColPrefix}_sum_${windowDays}d", F.sum(F.col(transactionAmountCol)).over(windowSpec))
      .withColumn(s"${outputColPrefix}_avg_${windowDays}d", F.avg(F.col(transactionAmountCol)).over(windowSpec))
      .withColumn(s"${outputColPrefix}_count_${windowDays}d", F.count(F.col(transactionAmountCol)).over(windowSpec))
  }
}

/**
 * Counts the number of transactions per user per month.
 * @param params Configuration parameters:
 *               `user_id_col` (String, default: "userid"): User identifier column.
 *               `transaction_date_col` (String, default: "transactiondate"): Transaction date/timestamp column.
 *               `output_col` (String, default: "user_monthly_transaction_count"): Output column for the monthly count.
 */
class UserMonthlyTransactionCounter(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val userIdCol: String = TransformerUtils.getStringParam(params, "user_id_col", "userid")
  val transactionDateCol: String = TransformerUtils.getStringParam(params, "transaction_date_col", "transactiondate")
  val outputCol: String = TransformerUtils.getStringParam(params, "output_col", "user_monthly_transaction_count")

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying UserMonthlyTransactionCounter. User ID: $userIdCol, Date: $transactionDateCol")
    val timestampCol = F.col(transactionDateCol).cast(TimestampType)
    // For total count per partition (user, year, month), orderBy should not be in the window for count(*)
    val windowSpec = Window.partitionBy(F.col(userIdCol), F.year(timestampCol), F.month(timestampCol))
    df.withColumn(outputCol, F.count("*").over(windowSpec))
  }
}

/**
 * Aggregates user spending per category, creating a new column for each category's sum for each user.
 * @param params Configuration parameters:
 *               `user_id_col` (String, default: "userid"): User identifier column.
 *               `category_col` (String, default: "merchantcategory"): Column containing categories.
 *               `transaction_amount_col` (String, default: "transactionamount"): Transaction amount column.
 *               `output_col_prefix` (String, default: "user_category_spend"): Prefix for new category spend columns.
 */
class UserCategoricalSpendAggregator(params: Map[String, Json]) extends FeatureTransformer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val userIdCol: String = TransformerUtils.getStringParam(params, "user_id_col", "userid")
  val categoryCol: String = TransformerUtils.getStringParam(params, "category_col", "merchantcategory")
  val transactionAmountCol: String = TransformerUtils.getStringParam(params, "transaction_amount_col", "transactionamount")
  val outputColPrefix: String = TransformerUtils.getStringParam(params, "output_col_prefix", "user_category_spend")

  override def apply(df: DataFrame): DataFrame = {
    logger.info(s"Applying UserCategoricalSpendAggregator. User ID: $userIdCol, Category: $categoryCol, Amount: $transactionAmountCol")
    
    // Using df.sparkSession.implicits._ for .as[String] might be needed if not available by default
    // However, DataFrame.select(...).distinct().collect() returns Array[Row], so map needs to handle Row
    val categories = df.select(categoryCol).distinct().collect().map(row => row.getString(0)).filterNot(_ == null).toList
    
    var currentDf = df
    categories.foreach { cat =>
      val sanitizedCatName = cat.toUpperCase.replaceAll("[^a-zA-Z0-9_]", "")
      val catOutputCol = s"${outputColPrefix}_${sanitizedCatName}_sum"
      currentDf = currentDf.withColumn(catOutputCol,
        F.sum(
          F.when(F.col(categoryCol) === cat, F.col(transactionAmountCol)).otherwise(F.lit(0.0)) // Ensure 'otherwise' is of compatible type
        ).over(Window.partitionBy(userIdCol))
      )
    }
    currentDf
  }
}
