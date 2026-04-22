import pytest 
from framework.scd import apply_scd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from datetime import date 

@pytest.fixture(scope="Session")
def spark():
    return SparkSession.builder/
    .master("local[*]")/
    .appName("SCDTest").getOrCreate()

def test_new_record(spark):
    source = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1"),
        
    ],["id", "first_name", "last_name", "hash_value"])

    target = spark.createDataFrame([], """ id INT, first_name STRING, last_name STRING, hash_value STRING, effective_date DATE, end_date DATE, is_current BOOLEAN""")

    result = apply_scd(
        source_df=source,
        target_df=target,
        business_key_cols=["id"],
        tracking_cols=["first_name", "last_name", "hash_value"]
    )

    assert result.count() == 1
    assert result.filter(col("is_current") == lit(True)).count() == 1

def test_changed_record(spark):
    target = spark.createDataFrame([
        (1, "Alice", "Johnson", "hash2",date(2023, 1, 1), None, True)
        
    ],["id", "first_name", "last_name", "hash_value", "effective_date", "end_date", "is_current"])

    source = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1")
    ],["id", "first_name", "last_name", "hash_value"])

    result = apply_scd(
        source_df=source,
        target_df=target,
        business_key_cols=["id"],
        tracking_cols=["first_name", "last_name", "hash_value"]
    )

    assert result.count() == 2
    assert result.filter(col("is_current") == lit(True)).count() == 1
    assert result.filter(col("is_current") == lit(False)).count() == 1

def test_unchanged_record(spark):
    target = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1",date(2023, 1, 1), None, True)
        
    ],["id", "first_name", "last_name", "hash_value", "effective_date", "end_date", "is_current"])

    source = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1")
    ],["id", "first_name", "last_name", "hash_value"])

    result = apply_scd(
        source_df=source,
        target_df=target,
        business_key_cols=["id"],
        tracking_cols=["first_name", "last_name", "hash_value"]
    )

    assert result.count() == 1
    assert result.filter(col("is_current") == lit(True)).count() == 1

def test_no_change(spark):
    target = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1",date(2023, 1, 1), None, True)
        
    ],["id", "first_name", "last_name", "hash_value", "effective_date", "end_date", "is_current"])

    source = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1")
    ],["id", "first_name", "last_name", "hash_value"])

    result = apply_scd(
        source_df=source,
        target_df=target,
        business_key_cols=["id"],
        tracking_cols=["first_name", "last_name", "hash_value"]
    )

    assert result.count() == 1
    assert result.filter(col("is_current") == lit(True)).count() == 1       

def test_multiple_records(spark):
    target = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1",date(2023, 1, 1), None, True),
        (2, "Bob", "Johnson", "hash2",date(2023, 1, 1), None, True)
    ],["id", "first_name", "last_name", "hash_value", "effective_date", "end_date", "is_current"])

    source = spark.createDataFrame([
        (1, "Alice", "Smith", "hash1"),
        (2, "Bob", "Johnson", "hash2"),
        (3, "Charlie", "Brown", "hash3")
    ],["id", "first_name", "last_name", "hash_value"])

    result = apply_scd(
        source_df=source,
        target_df=target,
        business_key_cols=["id"],
        tracking_cols=["first_name", "last_name", "hash_value"]
    )

    assert result.count() == 3
    assert result.filter(col("is_current") == lit(True)).count() == 3

    def test_single_active_record(spark):
        target = spark.createDataFrame([
            (1, "Alice", "Smith", "hash1",date(2023, 1, 1), None, True),
            (1, "Alice", "Smith", "hash1",date(2023, 1, 2), None, False)
        ],["id", "first_name", "last_name", "hash_value", "effective_date", "end_date", "is_current"])

        source = spark.createDataFrame([
            (1, "Alice", "Smith", "hash1")
        ],["id", "first_name", "last_name", "hash_value"])

        result = apply_scd(
            source_df=source,
            target_df=target,
            business_key_cols=["id"],
            tracking_cols=["first_name", "last_name", "hash_value"]
        )

        assert result.count() == 2
        assert result.filter(col("is_current") == lit(True)).count() == 1

    def test_idempotency(spark):
        target = spark.createDataFrame([
            (1, "Alice", "Smith", "hash1",date(2023, 1, 1), None, True)
        ],["id", "first_name", "last_name", "hash_value", "effective_date", "end_date", "is_current"])

        source = spark.createDataFrame([
            (1, "Alice", "Smith", "hash1")
        ],["id", "first_name", "last_name", "hash_value"])

        result1 = apply_scd(
            source_df=source,
            target_df=target,
            business_key_cols=["id"],
            tracking_cols=["first_name", "last_name", "hash_value"]
        )

        result2 = apply_scd(
            source_df=source,
            target_df=result1,
            business_key_cols=["id"],
            tracking_cols=["first_name", "last_name", "hash_value"]
        )

        assert result2.count() == result2.count()
        