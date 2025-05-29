package com.featureplatform

import java.time.LocalDateTime
import java.util.UUID

package object domain {
  // Base trait for all domain errors
  sealed trait DomainError extends Throwable {
    def message: String
    override def getMessage: String = message
  }

  // Transaction domain model
  final case class Transaction(
    id: UUID,
    amount: Double,
    currency: String,
    timestamp: LocalDateTime,
    customerId: String,
    merchantId: String,
    status: String,
    channel: String,
    category: String,
    metadata: Map[String, String] = Map.empty
  )

  // Feature definition
  final case class FeatureDefinition(
    name: String,
    description: String,
    featureType: FeatureType,
    defaultValue: Any,
    tags: Set[String] = Set.empty
  )

  // Feature types
  sealed trait FeatureType
  object FeatureType {
    case object Numeric extends FeatureType
    case object Categorical extends FeatureType
    case object Boolean extends FeatureType
    case object Timestamp extends FeatureType
  }

  // Feature value
  final case class FeatureValue(
    featureName: String,
    value: Any,
    timestamp: LocalDateTime,
    entityId: String
  )

  // Feature group
  final case class FeatureGroup(
    name: String,
    description: String,
    features: List[FeatureDefinition],
    primaryKey: String,
    timestampColumn: String,
    tags: Set[String] = Set.empty
  )
}
