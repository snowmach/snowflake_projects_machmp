import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window

def main(session: snowpark.Session):
    # Sample data to create the DataFrame
    data = [
        (1, "2023-01-01 00:00:00", "2023-01-01 01:00:00", "A", None, "X"),
        (1, "2023-01-01 00:00:00", "2023-01-01 02:00:00", "A", "B", "X"),
        (1, "2023-01-01 00:00:00", "2023-01-01 03:00:00", "C", "B", None),
        (1, "2023-01-01 00:00:00", "2023-01-01 03:00:00", None, None, "Y"),
        (1, "2023-01-01 00:00:00", "2023-01-01 03:00:00", None, "C", None),
        (2, "2023-01-02 00:00:00", "2023-01-02 01:00:00", "D", "E", None),
        (2, "2023-01-02 00:00:00", "2023-01-02 02:00:00", None, None, "F"),
    ]
    schema = ["CUSTID", "START_TMS", "LAST_CHG_TMS", "DATACOL1", "DATACOL2", "DATACOL3"]
    raw_df = session.create_dataframe(data, schema)

    # Define the list of columns to apply the forward-fill logic to.
    # You can easily add or remove column names here.
    data_cols_to_fill = ["DATACOL1", "DATACOL2", "DATACOL3"]
    
    # Define the key columns for partitioning and sorting
    key_cols = ["CUSTID", "START_TMS", "LAST_CHG_TMS"]

    # Define the window specification
    window_spec = (
        Window.partition_by("CUSTID", "START_TMS")
        .order_by("LAST_CHG_TMS")
        .rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
    )

    # Build the list of select expressions dynamically.
    # It starts with the key columns and adds the forward-filled data columns.
    select_exprs = key_cols + [
        F.last_value(c, ignore_nulls=True).over(window_spec).alias(c) 
        for c in data_cols_to_fill
    ]

    # Use the * operator to unpack the list of expressions into the select method
    final_df = raw_df.select(*select_exprs).sort(*key_cols)

    # Return the final DataFrame to display the results in the Snowflake worksheet
    return final_df
