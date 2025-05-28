from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    # Import functions if they are used in the type signature, otherwise not strictly needed in TYPE_CHECKING
    # from pyspark.sql.functions import col, year, current_date, lit 

class FeatureTransformer(ABC):
    """
    Abstract base class for feature transformers.
    A feature transformer encapsulates the logic to compute one or more features
    from an input Spark DataFrame.
    """

    @abstractmethod
    def apply(self, dataframe: "SparkDataFrame") -> "SparkDataFrame":
        """
        Applies the transformation logic to the input DataFrame.

        Args:
            dataframe: The input Spark DataFrame.

        Returns:
            A new Spark DataFrame with the transformed/computed features.
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


# --- Example Implementation ---

class SimpleAgeCalculator(FeatureTransformer):
    """
    A simple feature transformer that calculates age based on a birthdate column.
    """

    def __init__(self, birthdate_col: str, output_col: str = "age"):
        """
        Args:
            birthdate_col: Name of the column containing the birthdate (must be DateType or TimestampType).
            output_col: Name of the column to store the calculated age.
        """
        if not birthdate_col:
            raise ValueError("birthdate_col cannot be empty.")
        self.birthdate_col = birthdate_col
        self.output_col = output_col

    def apply(self, dataframe: "SparkDataFrame") -> "SparkDataFrame":
        """
        Calculates age from the birthdate column and adds it as a new column.
        Age is calculated as the difference in years from the current date.
        """
        from pyspark.sql.functions import col, year, current_date, months_between

        # Ensure the birthdate column exists
        if self.birthdate_col not in dataframe.columns:
            raise ValueError(f"Input DataFrame does not have column: {self.birthdate_col}")

        # Simple age calculation: years between birthdate and current_date
        # For a more precise age, one might consider day/month differences.
        # Using months_between and dividing by 12 for better accuracy than just year(date) - year(birthdate)
        age_col = (months_between(current_date(), col(self.birthdate_col)) / 12).cast("integer")
        
        return dataframe.withColumn(self.output_col, age_col)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(birthdate_col='{self.birthdate_col}', output_col='{self.output_col}')"

class WithGreeting(FeatureTransformer):
    """
    A simple transformer that adds a greeting column.
    """
    def __init__(self, greeting_col_name: str = "greeting", name_col: Optional[str] = None):
        self.greeting_col_name = greeting_col_name
        self.name_col = name_col


    def apply(self, dataframe: "SparkDataFrame") -> "SparkDataFrame":
        from pyspark.sql.functions import lit, col, concat_ws
        if self.name_col:
            if self.name_col not in dataframe.columns:
                raise ValueError(f"Name column '{self.name_col}' not found in DataFrame.")
            return dataframe.withColumn(self.greeting_col_name, concat_ws(" ", lit("Hello,"), col(self.name_col)))
        return dataframe.withColumn(self.greeting_col_name, lit("Hello, World!"))


if __name__ == "__main__":
    # Example Usage (requires a Spark session)
    # This part is for demonstration and won't run directly via subtask without Spark context.
    # It will be used in the later Spark job runner script.
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, DateType
        from datetime import date

        spark_example_session = SparkSession.builder.appName("FeatureTransformerExample").master("local[*]").getOrCreate()
        
        # Example 1: SimpleAgeCalculator
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("birth_date", DateType(), True)
        ])
        data = [("1", date(1990, 5, 15)), ("2", date(1985, 10, 20))]
        example_df = spark_example_session.createDataFrame(data, schema)

        age_calculator = SimpleAgeCalculator(birthdate_col="birth_date", output_col="calculated_age")
        transformed_df_age = age_calculator.apply(example_df)
        
        print("--- SimpleAgeCalculator Example ---")
        transformed_df_age.show()
        # Expected output (age will depend on current date):
        # +---+----------+----------------+
        # | id|birth_date|calculated_age|
        # +---+----------+----------------+
        # |  1|1990-05-15|              XX| # Age depends on current date
        # |  2|1985-10-20|              YY| # Age depends on current date
        # +---+----------+----------------+

        # Example 2: WithGreeting
        greeting_transformer = WithGreeting()
        transformed_df_greeting = greeting_transformer.apply(example_df) # Using example_df without name col
        print("\n--- WithGreeting Example (no name col) ---")
        transformed_df_greeting.show()
        # Expected output:
        # +---+----------+---------------+
        # | id|birth_date|       greeting|
        # +---+----------+---------------+
        # |  1|1990-05-15|Hello, World!|
        # |  2|1985-10-20|Hello, World!|
        # +---+----------+---------------+


        data_with_names = [("1", "Alice", date(1990, 5, 15)), ("2", "Bob", date(1985, 10, 20))]
        schema_with_names = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("birth_date", DateType(), True)
        ])
        example_df_names = spark_example_session.createDataFrame(data_with_names, schema_with_names)
        
        greeting_transformer_with_name = WithGreeting(name_col="name")
        transformed_df_greeting_named = greeting_transformer_with_name.apply(example_df_names)
        print("\n--- WithGreeting Example (with name col) ---")
        transformed_df_greeting_named.show()
        # Expected output:
        # +---+-----+----------+-------------+
        # | id| name|birth_date|     greeting|
        # +---+-----+----------+-------------+
        # |  1|Alice|1990-05-15|Hello, Alice|
        # |  2|  Bob|1985-10-20|  Hello, Bob|
        # +---+-----+----------+-------------+

        spark_example_session.stop()

    except ImportError:
        print("Pyspark is not installed. This example requires Pyspark.")
    except Exception as e:
        print(f"An error occurred during the example: {e}")
