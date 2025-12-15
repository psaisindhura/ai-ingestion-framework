from pyspark.sql.functions import col,lit, current_date

def apply_scd2(
        source_df,
        target_df,
        business_key_cols,
        tracking_cols,
        effective_date_col = "effective_date",
        end_date_col = "end_date",
        is_current_col = "is_current",
        hash_col = "hash_value"
):
    """Applies SCD Type 2 logic to the source and target DataFrames."""
    try:
        #Only current records from target
        target_current = target_df.filter(col(is_current_col) == lit(True))

        # Join source with current target 
        joined = (
            source_df.alias("src")
            .join(
                target_current.alias("tgt"),
                on = business_key_cols,
                how = "left"
                )
        )

        #record new or changed records
        new_or_changed = joined.filter(
            (col(f"tgt.{hash_col}").isNull) |
            (col(f"src.{hash_col}")!= col(f"tgt.{hash_col}"))
        )

        #Expire old records in target
        expired = (
            target_current
            .join(
                target_current
                .join(new_or_changed.select(business_key_cols), on=business_key_cols)
                .withColumn(end_date_col, current_date())
                .withColumn(is_current_col, lit(False)),
            )
        )

        #new active record 
        new_active = (
            new_or_changed.select("src.*")
            .withColumn(effective_date_col, current_date())
            .withColumn(end_date_col, lit(None).cast("date"))
            .withColumn(is_current_col, lit(True))
        )

        #unchanged records from target
        unchanged = target_current.join(
            expired.select(business_key_cols), on=business_key_cols, how="left_anti"
        )

        return unchanged.unionByName(expired).unionByName(new_active)
    except Exception as e:
        print("Error applying SCD2 logic:", e)
        raise e


