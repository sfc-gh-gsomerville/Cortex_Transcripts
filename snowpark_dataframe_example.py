from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, sum, avg, month, year, to_date
import pandas as pd
from config import CONNECTION_PARAMETERS

def create_session():
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    print("Connected to Snowflake!")
    return session

def main():
    try:
        # Connect to Snowflake
        session = create_session()
        
        # Create a sample DataFrame
        print("Creating sample data...")
        data = [
            (1, "Product A", "2023-01-15", 100.50, 10),
            (2, "Product B", "2023-01-20", 200.75, 5),
            (3, "Product C", "2023-02-10", 150.25, 8),
            (4, "Product A", "2023-02-15", 100.50, 12),
            (5, "Product B", "2023-03-05", 200.75, 6),
            (6, "Product C", "2023-03-20", 150.25, 9)
        ]
        
        # Create a DataFrame from the data
        schema = ["ID", "PRODUCT_NAME", "SALE_DATE", "PRICE", "QUANTITY"]
        df = session.create_dataframe(data, schema=schema)
        
        # Show the DataFrame
        print("\nSample DataFrame:")
        df.show()
        
        # Save to a temporary table
        temp_table_name = "TEMP_SALES_DATA"
        df.write.save_as_table(temp_table_name, mode="overwrite", table_type="temporary")
        print(f"\nSaved data to temporary table: {temp_table_name}")
        
        # Read from the table
        sales_df = session.table(temp_table_name)
        
        # Perform some transformations
        print("\nPerforming transformations...")
        
        # Calculate total sales amount
        sales_df = sales_df.with_column("TOTAL_AMOUNT", col("PRICE") * col("QUANTITY"))
        
        # Extract month and year from date
        sales_df = sales_df.with_column("MONTH", month(to_date(col("SALE_DATE"))))
        sales_df = sales_df.with_column("YEAR", year(to_date(col("SALE_DATE"))))
        
        # Show the transformed data
        print("\nTransformed DataFrame:")
        sales_df.show()
        
        # Group by analysis
        print("\nSales by Product:")
        product_sales = sales_df.group_by("PRODUCT_NAME").agg(
            sum("TOTAL_AMOUNT").alias("TOTAL_SALES"),
            avg("QUANTITY").alias("AVG_QUANTITY")
        ).sort("TOTAL_SALES", ascending=False)
        
        product_sales.show()
        
        # Group by month analysis
        print("\nMonthly Sales:")
        monthly_sales = sales_df.group_by("MONTH").agg(
            sum("TOTAL_AMOUNT").alias("MONTHLY_SALES")
        ).sort("MONTH")
        
        monthly_sales.show()
        
        # Convert to pandas for further analysis
        pandas_df = sales_df.to_pandas()
        print("\nConverted to pandas DataFrame:")
        print(pandas_df.head())
        
        # Cleanup
        session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()
        
        # Close the session
        session.close()
        print("\nSession closed successfully")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()