package com.example.featureplatform.features

import com.example.featureplatform.utils.SparkSessionTestWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json // For Json type and its methods like Json.fromString

import java.sql.Timestamp // Import SparkSession

class TransactionTransformersSpec extends AnyWordSpec with Matchers with SparkSessionTestWrapper {

  // Helper to check DataFrame content
  def assertDataFrameData(df: org.apache.spark.sql.DataFrame, expectedData: Seq[Row]): Unit = {
    df.collect().toList should contain theSameElementsAs expectedData
  }

  "TransformerUtils" should {
    // No Spark implicits needed for TransformerUtils tests
    val params = Map(
      "strKey" -> Json.fromString("testValue"),
      "intKey" -> Json.fromInt(123),
      "doubleKey" -> Json.fromDoubleOrNull(123.45),
      "listStrKey" -> Json.fromValues(List(Json.fromString("a"), Json.fromString("b"))),
      "emptyStrKey" -> Json.fromString("")
    )

    "getStringParam correctly extract string or default" in {
      TransformerUtils.getStringParam(params, "strKey", "default") shouldBe "testValue"
      TransformerUtils.getStringParam(params, "nonExistentKey", "default") shouldBe "default"
      TransformerUtils.getStringParam(params, "intKey", "default") shouldBe "default" // Type mismatch
    }

    "getOptionalStringParam correctly extract optional string" in {
      TransformerUtils.getOptionalStringParam(params, "strKey") shouldBe Some("testValue")
      TransformerUtils.getOptionalStringParam(params, "nonExistentKey") shouldBe None
      TransformerUtils.getOptionalStringParam(params, "intKey") shouldBe None // Type mismatch
    }
    
    "getDoubleParam correctly extract double or default" in {
      TransformerUtils.getDoubleParam(params, "doubleKey", 0.0) shouldBe 123.45
      TransformerUtils.getDoubleParam(params, "intKey", 0.0) shouldBe 123.0 // Int can be cast to Double
      TransformerUtils.getDoubleParam(params, "nonExistentKey", 0.0) shouldBe 0.0
      TransformerUtils.getDoubleParam(params, "strKey", 0.0) shouldBe 0.0 // Type mismatch
    }

    "getStringListParam correctly extract list of strings or default" in {
      TransformerUtils.getStringListParam(params, "listStrKey", Nil) shouldBe List("a", "b")
      TransformerUtils.getStringListParam(params, "nonExistentKey", List("default")) shouldBe List("default")
      TransformerUtils.getStringListParam(params, "strKey", Nil) shouldBe Nil // Type mismatch
    }
  }

  "TransactionIndicatorDeriver" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._ // Import from the stable val

    val transformer = new TransactionIndicatorDeriver(Map.empty) // Uses defaults
    val data = Seq(
      ("CREDIT", "txn1"),
      ("DEBIT", "txn2"),
      ("credit", "txn3"),
      ("other", "txn4"),
      (null, "txn5")
    ).toDF("creditdebitindicator", "id")

    val expectedSchema = StructType(data.schema.fields ++ Array(
      StructField("is_credit", BooleanType, false),
      StructField("is_debit", BooleanType, false)
    ))
    
    val transformedDf = transformer.apply(data)

    "add is_credit and is_debit columns" in {
      transformedDf.schema.fieldNames should contain allOf ("is_credit", "is_debit")
      transformedDf.schema("is_credit").dataType shouldBe BooleanType
      transformedDf.schema("is_debit").dataType shouldBe BooleanType
    }

    "correctly flag credit and debit transactions" in {
      val expectedData = Seq(
        Row("CREDIT", "txn1", true, false),
        Row("DEBIT", "txn2", false, true),
        Row("credit", "txn3", true, false), // F.upper handles this
        Row("other", "txn4", false, false), // Not CREDIT or DEBIT
        Row(null, "txn5", null, null)       // Null input results in null output for comparison
      )
      assertDataFrameData(transformedDf.select("creditdebitindicator", "id", "is_credit", "is_debit"), expectedData)
    }

    "work with custom column names" in {
      val customParams = Map(
        "credit_debit_indicator_col" -> Json.fromString("indicator"),
        "output_col_credit" -> Json.fromString("is_credited_custom"),
        "output_col_debit" -> Json.fromString("is_debited_custom")
      )
      val customTransformer = new TransactionIndicatorDeriver(customParams)
      val customData = Seq(("CREDIT", "id1")).toDF("indicator", "id")
      val customTransformedDf = customTransformer.apply(customData)

      customTransformedDf.schema.fieldNames should contain allOf ("is_credited_custom", "is_debited_custom")
      val resultRow = customTransformedDf.select("is_credited_custom", "is_debited_custom").first()
      resultRow.getBoolean(0) shouldBe true
      resultRow.getBoolean(1) shouldBe false
    }
  }

  "TransactionDatetimeDeriver" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val transformer = new TransactionDatetimeDeriver(Map.empty) // Uses defaults
    val ts1 = Timestamp.valueOf("2023-01-15 10:30:00") // Sunday, 10 AM
    val ts2 = Timestamp.valueOf("2023-01-16 22:15:00") // Monday, 22 PM
    val data = Seq(
      (ts1, "txn1"),
      (ts2, "txn2"),
      (null, "txn3")
    ).toDF("transactiontimestamp", "id")

    val transformedDf = transformer.apply(data)

    "add transaction_hour and transaction_day_of_week columns" in {
      transformedDf.schema.fieldNames should contain allOf ("transaction_hour", "transaction_day_of_week")
      transformedDf.schema("transaction_hour").dataType shouldBe IntegerType
      transformedDf.schema("transaction_day_of_week").dataType shouldBe IntegerType // Spark's dayofweek is Int (1=Sun, 7=Sat)
    }

    "correctly derive datetime features" in {
      // Day of week: Sunday=1, Monday=2, ..., Saturday=7 (Spark default)
      val expectedData = Seq(
        Row(ts1, "txn1", 10, 1), // Sunday is 1
        Row(ts2, "txn2", 22, 2), // Monday is 2
        Row(null, "txn3", null, null)
      )
      assertDataFrameData(transformedDf.select("transactiontimestamp", "id", "transaction_hour", "transaction_day_of_week"), expectedData)
    }
  }

  "TransactionStatusDeriver" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val params = Map(
      "status_values" -> Json.fromValues(List(Json.fromString("SUCCESS"), Json.fromString("FAILED")))
    )
    val transformer = new TransactionStatusDeriver(params)
    val data = Seq(
      ("SUCCESS", "txn1"),
      ("FAILED", "txn2"),
      ("success", "txn3"), 
      ("PENDING", "txn4"), 
      (null, "txn5")
    ).toDF("transactionstatus", "id")

    val transformedDf = transformer.apply(data)

    "add one-hot encoded status columns" in {
      transformedDf.schema.fieldNames should contain allOf ("transaction_status_SUCCESS", "transaction_status_FAILED")
      transformedDf.schema("transaction_status_SUCCESS").dataType shouldBe BooleanType
      transformedDf.schema("transaction_status_FAILED").dataType shouldBe BooleanType
      transformedDf.schema.fieldNames should not contain "transaction_status_PENDING"
    }

    "correctly flag defined statuses" in {
      val expectedData = Seq(
        Row("SUCCESS", "txn1", true, false),
        Row("FAILED", "txn2", false, true),
        Row("success", "txn3", true, false), 
        Row("PENDING", "txn4", false, false), // Not in defined status_values for one-hot
        Row(null, "txn5", null, null)       // Null input results in null for comparison-based flags
      )
      assertDataFrameData(transformedDf.select("transactionstatus", "id", "transaction_status_SUCCESS", "transaction_status_FAILED"), expectedData)
    }
  }
  
  "TransactionChannelDeriver" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val params = Map(
      "channel_values" -> Json.fromValues(List(Json.fromString("ATM"), Json.fromString("MOBILE_BANKING")))
    )
    val transformer = new TransactionChannelDeriver(params)
    val data = Seq(
      ("ATM", "txn1"),
      ("MOBILE_BANKING", "txn2"), 
      ("atm", "txn3"),
      ("POS", "txn4")
    ).toDF("transactionchannel", "id")

    val transformedDf = transformer.apply(data)
    
    "add one-hot encoded channel columns with sanitized names" in {
      transformedDf.schema.fieldNames should contain allOf ("transaction_channel_ATM", "transaction_channel_MOBILE_BANKING")
      transformedDf.schema("transaction_channel_ATM").dataType shouldBe BooleanType
    }
    
    "correctly flag defined channels" in {
       val expectedData = Seq(
        Row("ATM", "txn1", true, false),
        Row("MOBILE_BANKING", "txn2", false, true),
        Row("atm", "txn3", true, false),
        Row("POS", "txn4", false, false)
      )
      assertDataFrameData(transformedDf.select("transactionchannel", "id", "transaction_channel_ATM", "transaction_channel_MOBILE_BANKING"), expectedData)
    }
  }

  "TransactionValueDeriver" should {
    "use default parameters if none provided" in {
      val sparkSession = spark // Local stable val for SparkSession
      import sparkSession.implicits._

      val transformer = new TransactionValueDeriver(Map.empty)
      val data = Seq((100.0, "txn1"), (1500.0, "txn2")).toDF("transactionamount", "id")
      val transformedDf = transformer.apply(data)
      
      transformedDf.schema.fieldNames should contain ("is_high_value_transaction")
      val expectedData = Seq(Row(100.0, "txn1", false), Row(1500.0, "txn2", true))
      assertDataFrameData(transformedDf.select("transactionamount", "id", "is_high_value_transaction"), expectedData)
    }

    "use custom parameters when provided" in {
      val sparkSession = spark // Local stable val for SparkSession
      import sparkSession.implicits._

      val params = Map(
        "input_col" -> Json.fromString("amount"),
        "output_col" -> Json.fromString("is_large_tx"),
        "high_value_threshold" -> Json.fromDoubleOrNull(50.0)
      )
      val transformer = new TransactionValueDeriver(params)
      val data = Seq((25.0, "txn1"), (75.0, "txn2")).toDF("amount", "id")
      val transformedDf = transformer.apply(data)

      transformedDf.schema.fieldNames should contain ("is_large_tx")
      transformedDf.schema.fieldNames should not contain "is_high_value_transaction"
      val expectedData = Seq(Row(25.0, "txn1", false), Row(75.0, "txn2", true))
      assertDataFrameData(transformedDf.select("amount", "id", "is_large_tx"), expectedData)
    }
  }
  
  "TransactionModeDeriver" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val params = Map(
      "mode_values" -> Json.fromValues(List(Json.fromString("CARD"), Json.fromString("UPI")))
    )
    val transformer = new TransactionModeDeriver(params)
    val data = Seq(
      ("CARD", "txn1"),
      ("upi", "txn2"),
      ("CASH", "txn3")
    ).toDF("transactionmode", "id")
    val transformedDf = transformer.apply(data)

    "add one-hot encoded mode columns" in {
      transformedDf.schema.fieldNames should contain allOf ("transaction_mode_CARD", "transaction_mode_UPI")
    }

    "correctly flag defined modes" in {
      val expectedData = Seq(
        Row("CARD", "txn1", true, false),
        Row("upi", "txn2", false, true),
        Row("CASH", "txn3", false, false)
      )
      assertDataFrameData(transformedDf.select("transactionmode", "id", "transaction_mode_CARD", "transaction_mode_UPI"), expectedData)
    }
  }

  "TransactionCategoryDeriver" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val params = Map(
      "category_values" -> Json.fromValues(List(Json.fromString("GROCERIES"), Json.fromString("TRAVEL")))
    )
    val transformer = new TransactionCategoryDeriver(params)
    val data = Seq(
      ("GROCERIES", "txn1"),
      ("travel", "txn2"),
      ("UTILITIES", "txn3")
    ).toDF("merchantcategory", "id")
    val transformedDf = transformer.apply(data)

    "add one-hot encoded category columns" in {
      transformedDf.schema.fieldNames should contain allOf ("transaction_category_GROCERIES", "transaction_category_TRAVEL")
    }
    
    "correctly flag defined categories" in {
      val expectedData = Seq(
        Row("GROCERIES", "txn1", true, false),
        Row("travel", "txn2", false, true),
        Row("UTILITIES", "txn3", false, false)
      )
      assertDataFrameData(transformedDf.select("merchantcategory", "id", "transaction_category_GROCERIES", "transaction_category_TRAVEL"), expectedData)
    }
  }

  // More complex transformers
  "UserSpendAggregator" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val params = Map(
      "user_id_col" -> Json.fromString("user"),
      "transaction_amount_col" -> Json.fromString("amount"),
      "transaction_date_col" -> Json.fromString("ts"),
      "window_days" -> Json.fromInt(2), // Short window for testing
      "output_col_prefix" -> Json.fromString("user_agg")
    )
    val transformer = new UserSpendAggregator(params)
    val data = Seq(
      ("u1", Timestamp.valueOf("2023-01-01 10:00:00"), 10.0),
      ("u1", Timestamp.valueOf("2023-01-02 10:00:00"), 20.0), // Within 2 days of 01-03
      ("u1", Timestamp.valueOf("2023-01-03 10:00:00"), 30.0), 
      ("u1", Timestamp.valueOf("2023-01-04 10:00:00"), 40.0), 
      ("u2", Timestamp.valueOf("2023-01-01 10:00:00"), 5.0)
    ).toDF("user", "ts", "amount")

    val transformedDf = transformer.apply(data)
    // transformedDf.printSchema() 
    // transformedDf.show(false)

    "add aggregated spend columns" in {
      transformedDf.schema.fieldNames should contain allOf (
        "user_agg_sum_2d", "user_agg_avg_2d", "user_agg_count_2d"
      )
    }

    "calculate aggregations correctly" in {
      // For u1, ts=2023-01-03: window includes 01-01, 01-02, 01-03. Amounts: 10, 20, 30. Sum=60, Avg=20, Count=3
      // For u1, ts=2023-01-04: window includes 01-02, 01-03, 01-04. Amounts: 20, 30, 40. Sum=90, Avg=30, Count=3
      // Note: rangeBetween includes rows whose `orderBy` value is within [current - range, current].
      // For 2023-01-03 (dayNumber ~2), range is [2-2*1 = 0, 2]. So dayNumber 0,1,2. (01-01, 01-02, 01-03)
      // For 2023-01-04 (dayNumber ~3), range is [3-2*1 = 1, 3]. So dayNumber 1,2,3. (01-02, 01-03, 01-04)
      // Spark's long representation of timestamp is seconds since epoch.
      // 2 days = 2 * 24 * 60 * 60 = 172800 seconds.
      
      // For ts "2023-01-03 10:00:00" (u1): values are 10,20,30. Sum=60, Avg=20, Count=3
      // For ts "2023-01-04 10:00:00" (u1): values are 20,30,40. Sum=90, Avg=30, Count=3
      
      val resultU1_0103 = transformedDf.filter($"user" === "u1" && $"ts" === Timestamp.valueOf("2023-01-03 10:00:00")).select("user_agg_sum_2d", "user_agg_avg_2d", "user_agg_count_2d").first()
      resultU1_0103.getDouble(0) shouldBe 60.0
      resultU1_0103.getDouble(1) shouldBe 20.0
      resultU1_0103.getLong(2) shouldBe 3

      val resultU1_0104 = transformedDf.filter($"user" === "u1" && $"ts" === Timestamp.valueOf("2023-01-04 10:00:00")).select("user_agg_sum_2d", "user_agg_avg_2d", "user_agg_count_2d").first()
      resultU1_0104.getDouble(0) shouldBe 90.0
      resultU1_0104.getDouble(1) shouldBe 30.0
      resultU1_0104.getLong(2) shouldBe 3
      
      val resultU2 = transformedDf.filter($"user" === "u2").select("user_agg_sum_2d", "user_agg_avg_2d", "user_agg_count_2d").first()
      resultU2.getDouble(0) shouldBe 5.0
      resultU2.getDouble(1) shouldBe 5.0
      resultU2.getLong(2) shouldBe 1
    }
  }
  
  "UserMonthlyTransactionCounter" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val transformer = new UserMonthlyTransactionCounter(Map.empty) // uses defaults
    val data = Seq(
      ("u1", Timestamp.valueOf("2023-01-10 10:00:00")),
      ("u1", Timestamp.valueOf("2023-01-20 10:00:00")),
      ("u1", Timestamp.valueOf("2023-02-05 10:00:00")),
      ("u2", Timestamp.valueOf("2023-01-15 10:00:00"))
    ).toDF("userid", "transactiondate")

    val transformedDf = transformer.apply(data)
    // transformedDf.show()

    "add user_monthly_transaction_count column" in {
      transformedDf.schema.fieldNames should contain ("user_monthly_transaction_count")
      transformedDf.schema("user_monthly_transaction_count").dataType shouldBe LongType // Count is Long
    }

    "correctly count transactions per user per month" in {
      val expectedData = Seq(
        Row("u1", Timestamp.valueOf("2023-01-10 10:00:00"), 2L),
        Row("u1", Timestamp.valueOf("2023-01-20 10:00:00"), 2L),
        Row("u1", Timestamp.valueOf("2023-02-05 10:00:00"), 1L),
        Row("u2", Timestamp.valueOf("2023-01-15 10:00:00"), 1L)
      )
      assertDataFrameData(transformedDf, expectedData)
    }
  }

  "UserCategoricalSpendAggregator" should {
    val sparkSession = spark // Local stable val for SparkSession
    import sparkSession.implicits._

    val params = Map(
      "category_col" -> Json.fromString("category"),
      "output_col_prefix" -> Json.fromString("user_spend_cat")
    )
    val transformer = new UserCategoricalSpendAggregator(params) // uses some defaults
    val data = Seq(
      ("u1", "catA", 10.0),
      ("u1", "catA", 20.0),
      ("u1", "catB", 5.0),
      ("u2", "catA", 100.0),
      ("u2", "catC", 50.0),
      ("u1", null, 1.0) 
    ).toDF("userid", "category", "transactionamount")

    val transformedDf = transformer.apply(data)
    // transformedDf.show(false)

    "add sum columns for each category" in {
      // Distinct categories in data are catA, catB, catC (nulls filtered)
      // Sanitized names: CATA, CATB, CATC
      transformedDf.schema.fieldNames should contain allOf (
        "user_spend_cat_CATA_sum", 
        "user_spend_cat_CATB_sum",
        "user_spend_cat_CATC_sum"
      )
    }
    
    "calculate categorical spend correctly" in {
      // For u1: catA sum = 30, catB sum = 5, catC sum = 0
      // For u2: catA sum = 100, catB sum = 0, catC sum = 50
      val u1Data = transformedDf.filter($"userid" === "u1").distinct().select("userid", "user_spend_cat_CATA_sum", "user_spend_cat_CATB_sum", "user_spend_cat_CATC_sum").first()
      u1Data.getDouble(1) shouldBe 30.0
      u1Data.getDouble(2) shouldBe 5.0
      u1Data.getDouble(3) shouldBe 0.0
      
      val u2Data = transformedDf.filter($"userid" === "u2").distinct().select("userid", "user_spend_cat_CATA_sum", "user_spend_cat_CATB_sum", "user_spend_cat_CATC_sum").first()
      u2Data.getDouble(1) shouldBe 100.0
      u2Data.getDouble(2) shouldBe 0.0
      u2Data.getDouble(3) shouldBe 50.0
    }
  }
}
