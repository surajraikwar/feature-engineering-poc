package com.featureplatform.service

import cats.MonadError
import cats.effect.Sync
import cats.implicits._
import com.featureplatform.domain._
import com.featureplatform.repository.FeatureRepository
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

class FeatureService[F[_]: Sync](
  featureRepo: FeatureRepository[F],
  spark: SparkSession
) {
  import spark.implicits._
  
  private val F = Sync[F]
  
  def computeTransactionFeatures(transactions: Dataset[Transaction]): F[Dataset[FeatureValue]] = {
    F.delay {
      // Example feature: Total amount spent by customer
      val totalAmountFeatures = transactions
        .groupBy("customerId")
        .agg(sum("amount").as("total_amount"))
        .withColumn("featureName", lit("total_amount_spent"))
        .withColumn("value", col("total_amount"))
        .withColumn("timestamp", current_timestamp())
        .withColumnRenamed("customerId", "entityId")
        .select("featureName", "value", "timestamp", "entityId")
        .as[FeatureValue]
      
      // Example feature: Transaction count by customer
      val countFeatures = transactions
        .groupBy("customerId")
        .count()
        .withColumn("featureName", lit("transaction_count"))
        .withColumn("value", col("count").cast("double"))
        .withColumn("timestamp", current_timestamp())
        .withColumnRenamed("customerId", "entityId")
        .select("featureName", "value", "timestamp", "entityId")
        .as[FeatureValue]
      
      // Combine all features
      totalAmountFeatures.union(countFeatures)
    }
  }
  
  def saveFeatures(featureValues: Dataset[FeatureValue]): F[Unit] = {
    featureRepo.saveFeatureValues(featureValues)
  }
  
  def getFeatures(
    featureNames: List[String],
    entityIds: List[String],
    startTime: Option[LocalDateTime] = None,
    endTime: Option[LocalDateTime] = None
  ): F[Dataset[FeatureValue]] = {
    featureRepo.getFeatureValues(featureNames, entityIds, startTime, endTime)
  }
  
  def createFeatureGroup(featureGroup: FeatureGroup): F[Unit] = {
    featureRepo.createFeatureGroup(featureGroup)
  }
  
  def getFeatureGroup(name: String): F[Option[FeatureGroup]] = {
    featureRepo.getFeatureGroup(name)
  }
  
  def deleteFeatureGroup(name: String): F[Unit] = {
    featureRepo.deleteFeatureGroup(name)
  }
}

object FeatureService {
  def apply[F[_]: Sync](
    featureRepo: FeatureRepository[F],
    spark: SparkSession
  ): FeatureService[F] = new FeatureService[F](featureRepo, spark)
}
