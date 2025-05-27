# feature_platform/features/factory.py
import logging
from .transform import FeatureTransformer, SimpleAgeCalculator, WithGreeting
from .financial_transformers import (
    UserSpendAggregator,
    UserMonthlyTransactionCounter,
    UserCategoricalSpendAggregator
)

logger = logging.getLogger(__name__)

TRANSFORMER_REGISTRY = {
    "UserSpendAggregator": UserSpendAggregator,
    "UserMonthlyTransactionCounter": UserMonthlyTransactionCounter,
    "UserCategoricalSpendAggregator": UserCategoricalSpendAggregator,
    "SimpleAgeCalculator": SimpleAgeCalculator,
    "WithGreeting": WithGreeting,
    # Add other transformers here as they are created
}

def get_transformer(name: str, params: dict) -> FeatureTransformer:
    """
    Instantiates a FeatureTransformer based on its name and parameters.

    Args:
        name: The name of the transformer (must be a key in TRANSFORMER_REGISTRY).
        params: A dictionary of parameters to initialize the transformer.

    Returns:
        An instance of the requested FeatureTransformer.

    Raises:
        ValueError: If the transformer name is not found in the registry
                    or if instantiation fails.
    """
    TransformerClass = TRANSFORMER_REGISTRY.get(name)
    if not TransformerClass:
        logger.error(f"Unknown transformer: {name}. Available transformers: {list(TRANSFORMER_REGISTRY.keys())}")
        raise ValueError(f"Unknown transformer: {name}")

    try:
        logger.info(f"Initializing transformer: {name} with params: {params}")
        transformer_instance = TransformerClass(**params)
        return transformer_instance
    except Exception as e:
        # Catching TypeError from Pydantic if params don't match, or other init errors
        logger.error(f"Error initializing transformer {name} with params {params}: {e}", exc_info=True)
        # Consider if specific error types from transformers' __init__ should be handled differently
        raise ValueError(f"Failed to initialize transformer {name}: {e}")
