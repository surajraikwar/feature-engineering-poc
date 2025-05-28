from pyspark.sql import DataFrame as SparkDataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType
import logging

from .transform import FeatureTransformer # Assuming transform.py is in the same directory

logger = logging.getLogger(__name__)

class UserSpendAggregator(FeatureTransformer):
    """
    Aggregates user spend over a defined time window (e.g., last X days).
    Assumes input DataFrame has user_id, transaction_date/timestamp, and amount.
    """
    def __init__(self, 
                 user_id_col: str = "user_id", 
                 timestamp_col: str = "timestamp", # Expects DateType or TimestampType
                 amount_col: str = "amount",
                 window_days: int = 30,
                 total_spend_output_col_suffix: str = "_total_spend",
                 avg_spend_output_col_suffix: str = "_avg_spend"):
        if window_days <= 0:
            raise ValueError("window_days must be positive.")
        self.user_id_col = user_id_col
        self.timestamp_col = timestamp_col
        self.amount_col = amount_col
        self.window_days = window_days
        self.total_spend_output_col = f"user_{window_days}d{total_spend_output_col_suffix}"
        self.avg_spend_output_col = f"user_{window_days}d{avg_spend_output_col_suffix}"

    def apply(self, dataframe: SparkDataFrame) -> SparkDataFrame:
        logger.info(f"Applying UserSpendAggregator for {self.window_days}-day window.")

        # Ensure timestamp_col is of TimestampType for windowing functions
        # If it's DateType, convert to TimestampType.
        if isinstance(dataframe.schema[self.timestamp_col].dataType, DateType):
            dataframe = dataframe.withColumn(self.timestamp_col, F.to_timestamp(F.col(self.timestamp_col)))
        elif not isinstance(dataframe.schema[self.timestamp_col].dataType, TimestampType):
             raise TypeError(f"Timestamp column '{self.timestamp_col}' must be DateType or TimestampType.")


        # Window specification: Partition by user, order by timestamp,
        # and define a window frame of X days preceding the current row's timestamp.
        # Spark's time window functions typically operate on days as longs (seconds * minutes * hours * days)
        # The window is defined from current_date - X days up to current_date
        # This calculates a rolling aggregate for each transaction.
        # If a global aggregate (not per transaction) is needed, a group By after filtering would be different.
        # For "last 30-day spend *as of now*", we'd filter data first.
        # This implementation provides a rolling window spend *for each record*.
        
        # To get "total spend in the PRECEDING X days for each transaction":
        window_spec = (
            Window.partitionBy(self.user_id_col)
            .orderBy(F.col(self.timestamp_col).cast("long")) # Order by unix timestamp
            .rangeBetween(- (self.window_days -1) * 86400, Window.currentRow) # X days in seconds, including current day
        )
        # Note: Using (self.window_days - 1) for preceding days + current day = X days window.
        # If it should be strictly preceding (not including current day's transaction), adjust range.


        df_with_spend = dataframe.withColumn(
            self.total_spend_output_col, 
            F.sum(self.amount_col).over(window_spec)
        )
        
        # For average spend, we also need the count of transactions in the window
        # It might be more robust to count distinct days if multiple tx per day,
        # but for now, let's do count of transactions.
        df_with_spend = df_with_spend.withColumn(
            "_count_in_window",
            F.count(self.amount_col).over(window_spec)
        )

        df_with_spend = df_with_spend.withColumn(
            self.avg_spend_output_col,
            (F.col(self.total_spend_output_col) / F.greatest(F.lit(1), F.col("_count_in_window"))) # Avoid division by zero
        ).drop("_count_in_window")
        
        return df_with_spend

    def __repr__(self) -> str:
        return (f"{self.__class__.__name__}(user_id_col='{self.user_id_col}', "
                f"timestamp_col='{self.timestamp_col}', amount_col='{self.amount_col}', "
                f"window_days={self.window_days})")

class UserMonthlyTransactionCounter(FeatureTransformer):
    """
    Counts the number of transactions per user for each month.
    Adds a column with year-month and another with the transaction count for that month.
    """
    def __init__(self, 
                 user_id_col: str = "user_id",
                 timestamp_col: str = "timestamp", # Expects DateType or TimestampType
                 output_col_year_month: str = "year_month",
                 output_col_txn_count: str = "user_monthly_txn_count"):
        self.user_id_col = user_id_col
        self.timestamp_col = timestamp_col
        self.output_col_year_month = output_col_year_month
        self.output_col_txn_count = output_col_txn_count

    def apply(self, dataframe: SparkDataFrame) -> SparkDataFrame:
        logger.info("Applying UserMonthlyTransactionCounter.")
        
        # Create a year-month column
        df_with_year_month = dataframe.withColumn(
            self.output_col_year_month,
            F.date_format(F.col(self.timestamp_col), "yyyy-MM")
        )

        # Group by user and year-month to count transactions
        # This creates one row per user per month with their transaction count.
        # If the goal is to attach this count back to each original transaction,
        # a window function would be used instead. The current interpretation is one row per user-month.
        user_monthly_counts = (
            df_with_year_month.groupBy(self.user_id_col, self.output_col_year_month)
            .agg(F.count("*").alias(self.output_col_txn_count))
        )
        
        # To join this back to the original DataFrame (if needed, adds redundancy but attaches to each row)
        # This is often what's desired for feature vectors.
        # Ensure original dataframe doesn't already have output_col_year_month if we are re-adding it.
        # For safety, alias the year_month col from user_monthly_counts before join if there's a clash.
        
        # Let's assume we want to join it back.
        # If df_with_year_month already has unique rows per user-month, this join is fine.
        # If not, this might multiply rows if not careful.
        # A window function approach might be safer to add as a new column to original DF structure.
        
        # Using Window function approach to add count to each row:
        window_spec = Window.partitionBy(self.user_id_col, self.output_col_year_month)
        
        return df_with_year_month.withColumn(
            self.output_col_txn_count,
            F.count("*").over(window_spec)
        )

    def __repr__(self) -> str:
        return (f"{self.__class__.__name__}(user_id_col='{self.user_id_col}', "
                f"timestamp_col='{self.timestamp_col}')")


class UserCategoricalSpendAggregator(FeatureTransformer):
    """
    Aggregates user's total spend for a specific category.
    """
    def __init__(self, 
                 user_id_col: str = "user_id",
                 amount_col: str = "amount",
                 category_col: str = "category",
                 target_category_value: str = "food and drinks", # Value in category_col to filter by
                 output_col_name_prefix: str = "user_total_spend_"):
        self.user_id_col = user_id_col
        self.amount_col = amount_col
        self.category_col = category_col
        self.target_category_value = target_category_value
        # Sanitize target_category_value for use in column name
        safe_category_name = "".join(c if c.isalnum() else "_" for c in target_category_value.lower())
        self.output_col = f"{output_col_name_prefix}{safe_category_name}"


    def apply(self, dataframe: SparkDataFrame) -> SparkDataFrame:
        logger.info(f"Applying UserCategoricalSpendAggregator for category '{self.target_category_value}'.")

        if self.category_col not in dataframe.columns:
            raise ValueError(f"Category column '{self.category_col}' not found in DataFrame.")

        # Calculate spend for the target category per user
        # This will result in one row per user with their total spend in that category.
        user_category_spend = (
            dataframe.where(F.col(self.category_col) == self.target_category_value)
            .groupBy(self.user_id_col)
            .agg(F.sum(self.amount_col).alias(self.output_col))
        )

        # Join this aggregated spend back to the original DataFrame.
        # All rows for a given user will get the same value for this feature.
        # Use a left join to keep all original rows. Users with no spend in the category will get null.
        # Fill nulls with 0 for the aggregated spend.
        return dataframe.join(
            user_category_spend,
            on=self.user_id_col,
            how="left"
        ).fillna(0, subset=[self.output_col])

    def __repr__(self) -> str:
        return (f"{self.__class__.__name__}(user_id_col='{self.user_id_col}', "
                f"amount_col='{self.amount_col}', category_col='{self.category_col}', "
                f"target_category_value='{self.target_category_value}')")
