package com.featureplatform.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import com.typesafe.scalalogging.Logger

/**
 * Collection of transformer functions for transaction data.
 */
object TransactionTransformers {
  private val logger = Logger(getClass.getName)

  /**
   * Derives transaction status indicators
   */
  def deriveTransactionStatus(df: DataFrame): DataFrame = {
    logger.info("Deriving transaction status indicators")
    
    df.withColumn("is_successful", 
        when(col("transactionstatus") === "COMPLETED", true)
          .otherwise(false))
      .withColumn("is_failed", 
          when(col("transactionstatus") === "FAILED", true)
            .otherwise(false))
      .withColumn("is_pending", 
          when(col("transactionstatus") === "PENDING", true)
            .otherwise(false))
  }

  /**
   * Derives transaction channel indicators
   */
  def deriveTransactionChannel(df: DataFrame): DataFrame = {
    logger.info("Deriving transaction channel indicators")
    
    // Create indicator columns for each channel type
    val channels = Seq("ONLINE", "MOBILE", "BRANCH", "ATM")
    
    channels.foldLeft(df) { (currentDf, channel) =>
      currentDf.withColumn(s"is_${channel.toLowerCase}_txn", 
          when(lower(col("transactionchannel")) === channel.toLowerCase, true)
            .otherwise(false))
    }
  }

  /**
   * Derives transaction value indicators
   */
  def deriveTransactionValue(df: DataFrame, amountCol: String = "transactionamount", 
                            threshold: Double = 1000.0): DataFrame = {
    logger.info(s"Deriving transaction value indicators with threshold: $threshold")
    
    df.withColumn("is_high_value", 
        when(col(amountCol) >= threshold, true)
          .otherwise(false))
      .withColumn("amount_category",
        when(col(amountCol) < 100, "small")
          .when(col(amountCol) < 1000, "medium")
          .otherwise("large"))
  }

  /**
   * Derives transaction datetime features
   */
  def deriveDatetimeFeatures(df: DataFrame, timestampCol: String = "transactiontimestamp"): DataFrame = {
    logger.info("Deriving datetime features")
    
    df.withColumn("transaction_date", to_date(col(timestampCol)))
      .withColumn("transaction_day_of_week", date_format(col(timestampCol), "E"))
      .withColumn("transaction_hour", hour(col(timestampCol)))
      .withColumn("is_weekend", 
          when(dayofweek(col(timestampCol)).isin(1, 7), true)
            .otherwise(false))
  }

  /**
   * Derives transaction mode indicators
   */
  def deriveTransactionMode(df: DataFrame): DataFrame = {
    logger.info("Deriving transaction mode indicators")
    
    val modes = Seq("CARD", "WALLET", "CASH", "TRANSFER")
    
    modes.foldLeft(df) { (currentDf, mode) =>
      currentDf.withColumn(s"is_${mode.toLowerCase}_txn", 
          when(lower(col("transactionmode")) === mode.toLowerCase, true)
            .otherwise(false))
    }
  }

  /**
   * Derives merchant category indicators
   */
  def deriveMerchantCategories(df: DataFrame): DataFrame = {
    logger.info("Deriving merchant category indicators")
    
    val categories = Seq("RETAIL", "FOOD", "TRAVEL", "UTILITY", "ENTERTAINMENT", "HEALTH")
    
    categories.foldLeft(df) { (currentDf, category) =>
      currentDf.withColumn(s"is_${category.toLowerCase}_txn", 
          when(lower(col("merchantcategory")) === category.toLowerCase, true)
            .otherwise(false))
    }
  }

  /**
   * Derives country indicators
   */
  def deriveCountryIndicators(df: DataFrame): DataFrame = {
    logger.info("Deriving country indicators")
    
    val countries = Seq("US", "UK", "IN", "CA", "AU")
    
    countries.foldLeft(df) { (currentDf, country) =>
      currentDf.withColumn(s"is_${country.toLowerCase}_txn", 
          when(upper(col("country_code")) === country, true)
            .otherwise(false))
    }
  }
}

/**
 * Main transformer class that applies all transformations
 */
class TransactionFeatureTransformer(spark: SparkSession) {
  import spark.implicits._
  private val logger = Logger(getClass.getName)
  
  /**
   * Apply all available transformations to the input DataFrame
   */
  def transform(df: DataFrame): DataFrame = {
    logger.info("Starting transaction feature transformation")
    
    // Apply all transformations in sequence
    val transformations = Seq(
      TransactionTransformers.deriveTransactionStatus _,
      TransactionTransformers.deriveTransactionChannel _,
      TransactionTransformers.deriveTransactionValue(_, "transactionamount", 1000.0),
      TransactionTransformers.deriveDatetimeFeatures(_, "transactiontimestamp"),
      TransactionTransformers.deriveTransactionMode _,
      TransactionTransformers.deriveMerchantCategories _,
      TransactionTransformers.deriveCountryIndicators _
    )
    
    transformations.foldLeft(df) { (currentDf, transformFunc) =>
      transformFunc(currentDf)
    }
  }
}
