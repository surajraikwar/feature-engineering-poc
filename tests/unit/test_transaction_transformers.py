"""
Tests for transaction feature transformers.
"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, IntegerType

from domain.features.transaction_transformers import (
    TransactionIndicatorDeriver,
    TransactionDatetimeDeriver,
    TransactionStatusDeriver,
    TransactionChannelDeriver,
    TransactionValueDeriver,
    TransactionModeDeriver,
    TransactionCategoryDeriver
)
from datetime import datetime

# Assumes a spark_session fixture is available, e.g., from tests/conftest.py

def test_transaction_indicator_deriver(spark_session: SparkSession):
    """Tests TransactionIndicatorDeriver for credit/debit indicators."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("creditdebitindicator", StringType(), True)
    ])
    data = [
        Row(id="1", creditdebitindicator="CREDIT"),
        Row(id="2", creditdebitindicator="DEBIT"),
        Row(id="3", creditdebitindicator="UNKNOWN"),
        Row(id="4", creditdebitindicator=None)
    ]
    df = spark_session.createDataFrame(data, schema)

    transformer = TransactionIndicatorDeriver()
    transformed_df = transformer.transform(df)

    expected_data = [
        {"id": "1", "is_credit": True, "is_debit": False},
        {"id": "2", "is_credit": False, "is_debit": True},
        {"id": "3", "is_credit": False, "is_debit": False}, # Unknown
        {"id": "4", "is_credit": False, "is_debit": False}  # Null
    ]
    
    results = [row.asDict() for row in transformed_df.select("id", "is_credit", "is_debit").collect()]
    
    for r_dict in results:
        assert r_dict in expected_data
    assert len(results) == len(expected_data)


def test_transaction_datetime_deriver(spark_session: SparkSession):
    """Tests TransactionDatetimeDeriver for datetime features."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("transactiondatetime", TimestampType(), True)
    ])
    data = [
        Row(id="1", transactiondatetime=datetime(2023, 1, 15, 14, 30, 0)),  # Sunday, 2 PM
        Row(id="2", transactiondatetime=datetime(2023, 1, 16, 8, 0, 0)),   # Monday, 8 AM
        Row(id="3", transactiondatetime=datetime(2023, 1, 21, 23, 59, 59)) # Saturday, 11:59 PM
    ]
    df = spark_session.createDataFrame(data, schema)

    transformer = TransactionDatetimeDeriver()
    transformed_df = transformer.transform(df)

    expected_data = [
        {"id": "1", "hour": 14, "day_of_week": "Sunday", "is_weekend": True},
        {"id": "2", "hour": 8, "day_of_week": "Monday", "is_weekend": False},
        {"id": "3", "hour": 23, "day_of_week": "Saturday", "is_weekend": True}
    ]
    
    results = [
        row.asDict() for row in transformed_df.select(
            "id", 
            F.col("transaction_hour_of_day").alias("hour"), 
            F.col("transaction_day_of_week").alias("day_of_week"),
            F.col("is_weekend_transaction").alias("is_weekend")
        ).collect()
    ]

    for r_dict in results:
        assert r_dict in expected_data
    assert len(results) == len(expected_data)


def test_transaction_status_deriver(spark_session: SparkSession):
    """Tests TransactionStatusDeriver for successful transaction indicator."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("transactionstatus", StringType(), True)
    ])
    data = [
        Row(id="1", transactionstatus="SUCCESS"),
        Row(id="2", transactionstatus="FAILURE"),
        Row(id="3", transactionstatus="PENDING"),
        Row(id="4", transactionstatus=None)
    ]
    df = spark_session.createDataFrame(data, schema)

    transformer = TransactionStatusDeriver()
    transformed_df = transformer.transform(df)

    expected_data = [
        {"id": "1", "is_successful_transaction": True},
        {"id": "2", "is_successful_transaction": False},
        {"id": "3", "is_successful_transaction": False},
        {"id": "4", "is_successful_transaction": False} 
    ]
    results = [row.asDict() for row in transformed_df.select("id", "is_successful_transaction").collect()]
    
    for r_dict in results:
        assert r_dict in expected_data
    assert len(results) == len(expected_data)


def test_transaction_channel_deriver(spark_session: SparkSession):
    """Tests TransactionChannelDeriver for UPI transaction indicator."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("transactionchannel", StringType(), True)
    ])
    data = [
        Row(id="1", transactionchannel="OFF_APP_UPI"),
        Row(id="2", transactionchannel="ON_APP_upi"), # Mixed case
        Row(id="3", transactionchannel="CREDIT_CARD"),
        Row(id="4", transactionchannel="NET_BANKING"),
        Row(id="5", transactionchannel=None)
    ]
    df = spark_session.createDataFrame(data, schema)

    transformer = TransactionChannelDeriver()
    transformed_df = transformer.transform(df)

    expected_data = [
        {"id": "1", "is_upi_transaction": True},
        {"id": "2", "is_upi_transaction": True},
        {"id": "3", "is_upi_transaction": False},
        {"id": "4", "is_upi_transaction": False},
        {"id": "5", "is_upi_transaction": False} 
    ]
    results = [row.asDict() for row in transformed_df.select("id", "is_upi_transaction").collect()]

    for r_dict in results:
        assert r_dict in expected_data
    assert len(results) == len(expected_data)


def test_transaction_value_deriver(spark_session: SparkSession):
    """Tests TransactionValueDeriver for high-value transaction indicator."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("transactionamount", DoubleType(), True),
        StructField("alt_amount", DoubleType(), True)
    ])
    data = [
        Row(id="1", transactionamount=1500.0, alt_amount=1500.0),
        Row(id="2", transactionamount=500.0, alt_amount=500.0),
        Row(id="3", transactionamount=1000.0, alt_amount=1000.0), # Edge case: equal to default threshold
        Row(id="4", transactionamount=None, alt_amount=None)
    ]
    df = spark_session.createDataFrame(data, schema)

    # Test with default parameters
    transformer_default = TransactionValueDeriver()
    transformed_df_default = transformer_default.transform(df.select("id", "transactionamount"))
    
    expected_default = [
        {"id": "1", "is_high_value_transaction": True},  # 1500 > 1000
        {"id": "2", "is_high_value_transaction": False}, # 500 not > 1000
        {"id": "3", "is_high_value_transaction": False}, # 1000 not > 1000
        {"id": "4", "is_high_value_transaction": False}  # Null amount
    ]
    results_default = [row.asDict() for row in transformed_df_default.select("id", "is_high_value_transaction").collect()]
    for r_dict in results_default:
        assert r_dict in expected_default, f"Mismatch in default test: {r_dict}"
    assert len(results_default) == len(expected_default)

    # Test with custom threshold, input_col, and output_col
    custom_threshold = 500.0
    custom_input_col = "alt_amount"
    custom_output_col = "is_very_high_tx"
    transformer_custom = TransactionValueDeriver(
        high_value_threshold=custom_threshold,
        input_col=custom_input_col,
        output_col=custom_output_col
    )
    transformed_df_custom = transformer_custom.transform(df.select("id", "alt_amount"))
    
    expected_custom = [
        {"id": "1", "is_very_high_tx": True},  # 1500 > 500
        {"id": "2", "is_very_high_tx": False}, # 500 not > 500
        {"id": "3", "is_very_high_tx": True},  # 1000 > 500
        {"id": "4", "is_very_high_tx": False}  # Null amount
    ]
    results_custom = [row.asDict() for row in transformed_df_custom.select("id", custom_output_col).collect()]
    for r_dict in results_custom:
        assert r_dict in expected_custom, f"Mismatch in custom test: {r_dict}"
    assert len(results_custom) == len(expected_custom)

    # Test with missing input column
    transformer_missing_col = TransactionValueDeriver(input_col="non_existent_amount")
    with pytest.raises(ValueError, match="Input column 'non_existent_amount' not found"):
        transformer_missing_col.transform(df)


def test_transaction_mode_deriver(spark_session: SparkSession):
    """Tests TransactionModeDeriver for online/offline transaction mode."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("transactionchannel", StringType(), True)
    ])
    data = [
        Row(id="1", transactionchannel="OFF_APP_UPI"), # contains APP -> online
        Row(id="2", transactionchannel="ON_APP_IMPS"), # contains APP -> online
        Row(id="3", transactionchannel="NetBanking"),   # contains NET -> online
        Row(id="4", transactionchannel="net_banking_NEFT"), # contains net -> online
        Row(id="5", transactionchannel="ATM_WITHDRAWAL"), # offline
        Row(id="6", transactionchannel="BRANCH_CASH"),    # offline
        Row(id="7", transactionchannel=None)              # offline (null)
    ]
    df = spark_session.createDataFrame(data, schema)

    transformer = TransactionModeDeriver()
    transformed_df = transformer.transform(df)

    expected_data = [
        {"id": "1", "transaction_mode": "online"},
        {"id": "2", "transaction_mode": "online"},
        {"id": "3", "transaction_mode": "online"},
        {"id": "4", "transaction_mode": "online"},
        {"id": "5", "transaction_mode": "offline"},
        {"id": "6", "transaction_mode": "offline"},
        {"id": "7", "transaction_mode": "offline"}
    ]
    results = [row.asDict() for row in transformed_df.select("id", "transaction_mode").collect()]

    for r_dict in results:
        assert r_dict in expected_data
    assert len(results) == len(expected_data)


def test_transaction_category_deriver(spark_session: SparkSession):
    """Tests TransactionCategoryDeriver for refined category derivation."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("jupiterfinegraincategory", StringType(), True),
        StructField("jupiter_coarsegrain_category", StringType(), True)
    ])
    data = [
        Row(id="1", jupiterfinegraincategory="Food_Dining", jupiter_coarsegrain_category="Expenses"),
        Row(id="2", jupiterfinegraincategory="", jupiter_coarsegrain_category="Travel"), # Fine grain empty
        Row(id="3", jupiterfinegraincategory=None, jupiter_coarsegrain_category="Shopping"),# Fine grain null
        Row(id="4", jupiterfinegraincategory="", jupiter_coarsegrain_category=""), # Both empty
        Row(id="5", jupiterfinegraincategory=None, jupiter_coarsegrain_category=None), # Both null
        Row(id="6", jupiterfinegraincategory="Bills", jupiter_coarsegrain_category=""),
        Row(id="7", jupiterfinegraincategory="Bills", jupiter_coarsegrain_category=None),
    ]
    df = spark_session.createDataFrame(data, schema)

    transformer = TransactionCategoryDeriver()
    transformed_df = transformer.transform(df)

    expected_data = [
        {"id": "1", "refined_transaction_category": "Food_Dining"},
        {"id": "2", "refined_transaction_category": "Travel"},
        {"id": "3", "refined_transaction_category": "Shopping"},
        {"id": "4", "refined_transaction_category": "uncategorized"},
        {"id": "5", "refined_transaction_category": "uncategorized"},
        {"id": "6", "refined_transaction_category": "Bills"},
        {"id": "7", "refined_transaction_category": "Bills"},
    ]
    results = [row.asDict() for row in transformed_df.select("id", "refined_transaction_category").collect()]

    for r_dict in results:
        assert r_dict in expected_data
    assert len(results) == len(expected_data)
