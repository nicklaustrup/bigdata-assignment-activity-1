import pandas as pd
import sqlite3

def process_data():
    print("Starting data processing...")

    # Loading the transactions data from the CSV file into a pandas DataFrame
    file_path = r"src/data/transactions.csv" 
    df = pd.read_csv(file_path, encoding="utf-8")
    
    print("transactions.csv loaded")
    
    # Removing any rows with missing values in the DataFrame (Use dropna or another method)
    df.dropna(inplace=True)  # You can change this to other methods if required

    # Converting the 'TransactionDate' column to a datetime format using pandas
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    # Setting up a connection to SQLite database and create a table if it doesn't exist
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()
    
    print("Database connected")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product TEXT,
        amount REAL,
        TransactionDate TEXT,
        PaymentMethod TEXT,
        City TEXT,
        Category TEXT
    )
    """)
    
    print("Transactions table created")
    
    # Insert data into the database
    # Your task: Insert the cleaned DataFrame into the SQLite database. Ensure to replace the table if it already exists.
    df.to_sql("transactions", con=conn, if_exists='replace')


    # Query 1: Top 5 Most Sold Products
    # Your task: Write an SQL query to find the top 5 most sold products based on transaction count.
    
    print("1: Top 5 Most Sold Products")
    cursor.execute("""  
                    SELECT Product, COUNT(Product) AS num_sold
                    FROM transactions
                    GROUP BY Product
                    ORDER BY num_sold DESC
                    LIMIT 5
                   """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)


    # Query 2: Monthly Revenue Trend
    # Your task: Write an SQL query to find the total revenue per month.
    
    print("2: Monthly Revenue Trend")
    cursor.execute("""  
                    SELECT SUM(Amount) as Revenue,  strftime("%Y-%m", TransactionDate) as Month
                    FROM transactions
                    GROUP BY Month
                   """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)


    # Query 3: Payment Method Popularity
    # Your task: Write an SQL query to find the popularity of each payment method used in transactions.
    
    print("3: Payment Method Popularity")
    cursor.execute("""  
                    SELECT PaymentMethod, COUNT(PaymentMethod) as Times_Used
                    FROM transactions
                    GROUP BY PaymentMethod
                    ORDER BY Times_Used DESC
                   """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)


    # Query 4: Top 5 Cities with Most Transactions
    # Your task: Write an SQL query to find the top 5 cities with the most transactions.
    
    print("4: Top 5 Cities with Most Transactions")
    cursor.execute("""  
                    SELECT City, COUNT(City) as City_Transactions
                    FROM transactions
                    GROUP BY City
                    ORDER BY City_Transactions DESC
                    LIMIT 5
                   """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)


    # Query 5: Top 5 High-Spending Customers
    # Your task: Write an SQL query to find the top 5 customers who spent the most in total.

    print("5: Top 5 High-Spending Customers")
    cursor.execute("""  
                    SELECT CustomerID, SUM(Amount) as Spent
                    FROM transactions
                    GROUP BY CustomerID
                    ORDER BY Spent DESC
                    LIMIT 5
                   """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # Query 6: Hadoop vs Spark Related Product Sales
    # Your task: Write an SQL query to categorize products related to Hadoop and Spark and find their sales.
    
    print("6: Hadoop vs Spark Related Product Sales")
    # Option 1
    # Returns Spark Intro, Spark Advanced, etc
    # cursor.execute("""  
    #                 SELECT Product, SUM(Amount) as Total_Sales
    #                 FROM transactions
    #                 WHERE Product LIKE '%Hadoop%' OR Product LIKE '%Spark%'
    #                 GROUP BY Product
    #                 ORDER BY Total_Sales DESC
    #                """)
    
    # Option 2
    # Returns Spark and Hadoop
    cursor.execute("""  
                SELECT 
                    CASE
                        WHEN Product LIKE '%Spark%' THEN 'Spark'
                        WHEN Product LIKE '%Hadoop%' THEN 'Hadoop'
                    END AS Product_Category, 
                    SUM(Amount) as Total_Sales
                FROM transactions
                WHERE Product LIKE '%Hadoop%' OR Product LIKE '%Spark%'
                GROUP BY Product_Category
                ORDER BY Total_Sales DESC
                """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)


    # Query 7: Top Spending Customers in Each City
    # Your task: Write an SQL query to find the top spending customer in each city using subqueries.
    
    print("7: Top Spending Customers in Each City")
    
    # subquery A: per customer per city spending table
        # group by both city and customer
        # compute each customer's total spend inside that city 
    # cursor.execute("""
    #                 SELECT
    #                     city_totals.City,
    #                     city_totals.CustomerID,
    #                     city_totals.TotalSpent
    #                 FROM (
    #                     SELECT
    #                         City,
    #                         CustomerID,
    #                         SUM(Amount) AS TotalSpent
    #                     FROM transactions
    #                     GROUP BY City, CustomerID
    #                 ) as city_totals
    #                 """)
        
    
    # subquery B: per city per maximum spending table
        # start from subquery A
        # group by city
        # compute max total spend for each city
    # cursor.execute("""   
    #                 SELECT
    #                     City,
    #                     MAX(TotalSpent) AS MaxSpent
    #                 FROM (
    #                     SELECT
    #                         City,
    #                         CustomerID,
    #                         SUM(Amount) AS TotalSpent
    #                     FROM transactions
    #                     GROUP BY City, CustomerID
    #                 ) AS per_customer_city
    #                 GROUP BY City
    #                 """)

    # Match A and B
    # Join on city and total spend = max spend
    cursor.execute("""
                    SELECT
                        city_totals.City,
                        city_totals.CustomerID,
                        city_totals.TotalSpent
                    FROM (
                        SELECT
                            City,
                            CustomerID,
                            SUM(Amount) AS TotalSpent
                        FROM transactions
                        GROUP BY City, CustomerID
                    ) AS city_totals
                    JOIN (
                        SELECT
                            City,
                            MAX(TotalSpent) AS MaxSpent
                        FROM (
                            SELECT
                                City,
                                CustomerID,
                                SUM(Amount) AS TotalSpent
                            FROM transactions
                            GROUP BY City, CustomerID
                        ) AS per_customer_city
                        GROUP BY City
                    ) AS city_max
                    ON city_totals.City = city_max.City
                    AND city_totals.TotalSpent = city_max.MaxSpent
                    ORDER BY city_totals.City, city_totals.CustomerID
                    """)
        
    rows = cursor.fetchall()
    for row in rows:
        print(row)


    # Step 8: Close the connection
    # Your task: After all queries, make sure to commit any changes and close the connection
    conn.commit()
    conn.close()
    print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()
