"""
The domain.features module provides tools and implementations for
feature transformations, including base transformer classes, specific feature
transformer logic, and factories for instantiating transformers.
"""
from .transform import FeatureTransformer, SimpleAgeCalculator, WithGreeting
from .transaction_transformers import (
    UserSpendAggregator, 
    UserMonthlyTransactionCounter, 
    UserCategoricalSpendAggregator,
    TransactionIndicatorDeriver,
    TransactionDatetimeDeriver,
    TransactionStatusDeriver,
    TransactionChannelDeriver,
    TransactionValueDeriver,
    TransactionModeDeriver,
    TransactionCategoryDeriver
)
from .factory import TRANSFORMER_REGISTRY, get_transformer # New line

__all__ = [
    "FeatureTransformer",
    "SimpleAgeCalculator",
    "WithGreeting",
    "UserSpendAggregator",
    "UserMonthlyTransactionCounter",
    "UserCategoricalSpendAggregator",
    "TransactionIndicatorDeriver",
    "TransactionDatetimeDeriver",
    "TransactionStatusDeriver",
    "TransactionChannelDeriver",
    "TransactionValueDeriver",
    "TransactionModeDeriver",
    "TransactionCategoryDeriver",
    "TRANSFORMER_REGISTRY", # Exporting the registry itself
    "get_transformer",      # Exporting the factory function
]
