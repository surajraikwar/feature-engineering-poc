package com.featureplatform.config

import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import cats.effect.{Async, Blocker, ContextShift, Resource, Sync}
import cats.syntax.functor._
import java.nio.file.Paths

final case class DatabaseConfig(
  url: String,
  driver: String,
  user: String,
  password: String,
  connectionPoolSize: Int = 10
)

final case class SparkConfig(
  appName: String,
  master: String,
  shufflePartitions: Int,
  delta: DeltaConfig
)

final case class DeltaConfig(
  logRetentionDuration: String = "30 days",
  checkpointRetentionDuration: String = "30 days",
  autoCompact: Boolean = true,
  optimizeWrite: Boolean = true
)

final case class FeatureStoreConfig(
  catalog: String,
  database: String,
  basePath: String
)

final case class AppConfig(
  database: DatabaseConfig,
  spark: SparkConfig,
  featureStore: FeatureStoreConfig
)

object AppConfig {
  def load[F[_]: Async: ContextShift](configPath: String = "application.conf"): Resource[F, AppConfig] = {
    Blocker[F].flatMap { blocker =>
      Resource.liftF {
        ConfigSource.file(Paths.get(configPath))
          .loadF[F, AppConfig](blocker)
          .handleErrorWith { _ =>
            Sync[F].raiseError(new RuntimeException(s"Failed to load configuration from $configPath"))
          }
      }
    }
  }
}
