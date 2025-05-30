package com.example.featureplatform.sources

import com.example.featureplatform.config.SourceDefinition
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Trait defining the contract for data source readers.
 * Implementations of this trait are responsible for reading data from a specific
 * type of source based on a [[SourceDefinition]].
 */
trait DataSourceReader {
  /**
   * Reads data from a source as defined by the [[SourceDefinition]].
   *
   * @param spark The active SparkSession.
   * @param sourceDefinition The definition of the source to read.
   * @return Either a Throwable on error, or a DataFrame containing the source data.
   */
  def read(spark: SparkSession, sourceDefinition: SourceDefinition): Either[Throwable, DataFrame]
}
