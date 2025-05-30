package com.example.featureplatform.features

import scala.util.Try

// Correcting the import from JobConfig to FeatureTransformerConfig
import com.example.featureplatform.config.FeatureTransformerConfig

/**
 * Factory object for creating [[FeatureTransformer]] instances based on configuration.
 */
object TransformerFactory {
  /**
   * Creates a [[FeatureTransformer]] instance based on the provided [[FeatureTransformerConfig]].
   *
   * @param config The configuration specifying the transformer's name and parameters.
   * @return Either a Throwable on error (e.g., unknown transformer name),
   *         or the instantiated [[FeatureTransformer]].
   */
  def getTransformer(config: FeatureTransformerConfig): Either[Throwable, FeatureTransformer] = {
    Try {
      val params: Map[String, Json] = config.params
      config.name match {
        case "TransactionIndicatorDeriver" => new TransactionIndicatorDeriver(params)
        case "TransactionDatetimeDeriver" => new TransactionDatetimeDeriver(params)
        case "TransactionStatusDeriver" => new TransactionStatusDeriver(params)
        case "TransactionChannelDeriver" => new TransactionChannelDeriver(params)
        case "TransactionValueDeriver" => new TransactionValueDeriver(params)
        case "TransactionModeDeriver" => new TransactionModeDeriver(params)
        case "TransactionCategoryDeriver" => new TransactionCategoryDeriver(params)
        case "UserSpendAggregator" => new UserSpendAggregator(params)
        case "UserMonthlyTransactionCounter" => new UserMonthlyTransactionCounter(params)
        case "UserCategoricalSpendAggregator" => new UserCategoricalSpendAggregator(params)
        // Add other transformers here as they are implemented
        case _ => throw new IllegalArgumentException(s"Unknown transformer name: ${config.name}")
      }
    }.toEither // Converts Try[FeatureTransformer] to Either[Throwable, FeatureTransformer]
  }
}
