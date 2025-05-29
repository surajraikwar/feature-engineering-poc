package com.example.featureplatform.features

import org.apache.spark.sql.DataFrame

/**
 * Base trait for all feature transformers.
 * Each transformer takes a DataFrame as input and returns a transformed DataFrame.
 * Implementations must be Serializable as they might be used in Spark closures.
 */
trait FeatureTransformer extends Serializable {
  /**
   * Applies the feature transformation to the input DataFrame.
   * @param dataframe The input DataFrame.
   * @return The transformed DataFrame.
   */
  def apply(dataframe: DataFrame): DataFrame
}
