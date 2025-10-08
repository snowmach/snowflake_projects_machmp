# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
from snowflake.snowpark.window import Window
from snowflake.snowpark import functions as F
from snowflake.snowpark.session import Session

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 

    # 1. Use the same sample DataFrame
    data = [
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 10:00:00.000000"),
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 10:15:00.000000"), # Record A
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 11:15:00.000000"), # Record A
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 12:15:00.000000"), # Record A
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 13:15:00.000000"), # Record A
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 14:15:00.000000"), # Record A
        (101, "2025-10-08 00:00:00.000000", "2025-10-08 15:15:00.000000"), # Record A
        (101, "2025-10-09 00:00:00.000000", "2025-10-09 10:18:00.000000"), # Record B
        (101, "2025-10-09 00:00:00.000000", "2025-10-09 15:18:00.000000"), # Record B
        (101, "2025-10-10 00:00:00.000000", "2025-10-10 10:30:00.000000"), # Record C
        (202, "2025-11-01 00:00:00.000000", "2025-11-01 09:00:00.000000"),
        (202, "2025-11-01 00:00:00.000000", "2025-11-01 09:30:00.000000")
    ]
    schema = ["CUST_ID", "START_TMS", "LAST_CHG_TMS"]
    df = session.create_dataframe(data, schema) \
        .withColumn("START_TMS", F.to_timestamp(F.col("START_TMS"))) \
        .withColumn("LAST_CHG_TMS", F.to_timestamp(F.col("LAST_CHG_TMS")))    
    
    # 2. Define two window specifications
    # This window establishes the overall order of records for a customer
    base_window = Window.partitionBy("CUST_ID").orderBy("START_TMS", "LAST_CHG_TMS")
    
    # This window groups records with the same START_TMS to propagate the correct value
    group_window = Window.partitionBy("CUST_ID", "START_TMS").orderBy("LAST_CHG_TMS") \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    
    # 3. Chain the logic to derive the correct next timestamp
    # First, get the immediate next timestamp (intermediate step)
    df_with_next_tms = df.withColumn("NEXT_TMS_RAW", F.lead("START_TMS").over(base_window))
    
    # Now, propagate the correct timestamp across the group
    # The last_value in the group is the one we want for all members of the group
    df_with_final_tms = df_with_next_tms.withColumn(
        "NEXT_DISTINCT_TMS", F.last_value("NEXT_TMS_RAW").over(group_window)
    )
    
    # 4. Derive the final END_TMS column
    df_final = df_with_final_tms.withColumn(
        "END_TMS",
        F.when(
            F.col("NEXT_DISTINCT_TMS").isNotNull(),
            F.expr("DATEADD(microsecond, -1, NEXT_DISTINCT_TMS)")
        ).otherwise(
            F.to_timestamp(F.lit(None))
        )
    ).drop("NEXT_TMS_RAW", "NEXT_DISTINCT_TMS") # Clean up intermediate columns
    
    # Display the final result
    return df_final.sort("CUST_ID", "START_TMS", "LAST_CHG_TMS")    
