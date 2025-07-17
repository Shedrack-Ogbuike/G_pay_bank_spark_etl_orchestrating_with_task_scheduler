# Installing pyspark on a local machine
# import Necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
import os
import psycopg2
pip install python-dotenv


# Initialize Spark session
spark = SparkSession.builder \
    .appName("BanksETL") \
    .master("local[*]") \
    .config("spark.jars", r"C:\Users\user\Desktop\G_pay_bank\postgresql-42.7.7.jar") \
    .getOrCreate()
    
# Extract  this history data into a spark dataframe
df = spark.read.csv(r"dataset\rawdata\G_pay_bank_transactions.csv", header=True, inferSchema=True)

# Data cleaning and transformation
for col in df.columns:
    print(f"Column: {col}, Nulls: {df.filter(df[col].isNull()).count()}")


# Fill up the missing values
df_cleaned = df.fillna({
    "Customer_Name": "Unknown",
    "Customer_Address": "Unknown",
    "Customer_City": "Unknown",
    "Customer_State": "Unknown",
    "Customer_Country": "Unknown",
    "Company": "Unknown",
    "Job_Title": "Unknown",
    "Email": "Unknown",
    "Phone_Number": "Unknown",
    "Credit_Card_Number": 0,
    "IBAN": "Unknown",
    "Currency_Code": "Unknown",
    "Random_Number": 0.0,
    "Category": "Unknown",
    "Group": "Unknown",
    "Is_Active": "Unknown",
    "Description": "No Description",
    "Gender": "Unknown",
    "Marital_Status": "Unknown"
})
    
    
# drop the missing values in the Last_Updated column
df_cleaned = df_cleaned.dropna(subset=["Last_Updated"])

# Data cleaning and transformation
for col in df_cleaned.columns:
    print(f"Column: {col}, 'Nulls: ', {df_cleaned.filter(df_cleaned[col].isNull()).count()}")
 
 
# Data transformation
# Transaction table
transactions = df_cleaned.select("Transaction_Date","Amount","Transaction_Type")\
                         .withColumn("Transaction_ID", monotonically_increasing_id())\
                         .select("Transaction_ID", "Transaction_Date", "Amount", "Transaction_Type")


# customer table
customers = df_cleaned.select( "Customer_Name", "Customer_Address", "Customer_City", 
                               "Customer_State", "Customer_Country")\
                     .withColumn("Customer_ID", monotonically_increasing_id())\
                        .select("Customer_ID", "Customer_Name", "Customer_Address", 
                                "Customer_City", "Customer_State", "Customer_Country")
                     
                     
# employee table
employees = df_cleaned.select("company", "job_title", "email", "phone_number", "Gender", "Marital_Status")\
                        .withColumn("Employee_ID", monotonically_increasing_id())\
                        .select("Employee_ID", "company", "job_title", "email", "phone_number",  "Gender", "Marital_Status")
                        
                                  
# fact table
fact_table = df_cleaned.join(transactions, ["Transaction_Date","Amount","Transaction_Type"], how="inner")\
    .join(customers, [ "Customer_Name", "Customer_Address", "Customer_City", 
                               "Customer_State", "Customer_Country"], how="inner")\
    .join(employees, ["company", "job_title", "email", "phone_number", "Gender", "Marital_Status"] , how="inner")\
    .select('Transaction_ID', 'Customer_ID', 'Employee_ID','Transaction_Date',  'Credit_Card_Number',\
     'IBAN','Currency_Code','Random_Number','Category','Group','Is_Active','Last_Updated','Description')
                         
                         
 fact_table = fact_table.withColumn(
    'Is_Active', 
    (col('Is_Active') == 'True').cast('boolean')
)    
 
 from dotenv import load_dotenv
import os
load_dotenv()

# Get the password
db_password = os.getenv("DB_PASSWORD")

# Data loading
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="g_pay_bank",
        user="postgres",
        password=db_password
    )
    return conn

# connect to the database
conn = get_db_connection()
 
 
 # create the tables in the database
def create_table():
    cursor = conn.cursor()
    create_table_query = """
        DROP TABLE IF EXISTS fact_table;
        DROP TABLE IF EXISTS transactions;
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS employees;

        CREATE TABLE transactions (
            Transaction_ID BIGINT PRIMARY KEY,
            Transaction_Date DATE NOT NULL,
            Amount DECIMAL(18, 4) NOT NULL,
            Transaction_Type VARCHAR(50) NOT NULL
        );

        CREATE TABLE customers (
            Customer_ID BIGINT PRIMARY KEY,
            Customer_Name VARCHAR(1000) NOT NULL,
            Customer_Address VARCHAR(2550) NOT NULL,
            Customer_City VARCHAR(1000) NOT NULL,
            Customer_State VARCHAR(1000) NOT NULL,
            Customer_Country VARCHAR(1000) NOT NULL
        );

        CREATE TABLE employees (
            Employee_ID BIGINT PRIMARY KEY,
            Company VARCHAR(1000) NOT NULL,
            Job_Title VARCHAR(1000) NOT NULL,
            Email VARCHAR(1000) NOT NULL,
            Phone_Number VARCHAR(200) NOT NULL,
            Gender VARCHAR(50) NOT NULL,
            Marital_Status VARCHAR(200) NOT NULL
        );

        CREATE TABLE fact_table (
            Transaction_ID BIGINT REFERENCES transactions(Transaction_ID),
            Customer_ID BIGINT REFERENCES customers(Customer_ID),
            Employee_ID BIGINT REFERENCES employees(Employee_ID),
            Transaction_Date DATE NOT NULL,
            Credit_Card_Number VARCHAR(200) NOT NULL,
            IBAN VARCHAR(34) NOT NULL,
            Currency_Code VARCHAR(100) NOT NULL,
            Random_Number DOUBLE PRECISION NOT NULL,
            Category VARCHAR(1000) NOT NULL,
            "Group" VARCHAR(1000) NOT NULL,
            Is_Active BOOLEAN NOT NULL,
            Last_Updated TIMESTAMP NOT NULL,
            Description TEXT
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


 url = "jdbc:postgresql://localhost:5432/g_pay_bank"
properties = {
    "user": "postgres",
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# Load data into the database
transactions.write.jdbc(url=url, table="transactions", mode="append", properties=properties)



                          customers.write.jdbc(url=url, table="customers", mode="append", properties=properties)
                          
                          employees.write.jdbc(url=url, table="employees", mode="append", properties=properties)
                          
                          fact_table.write.jdbc(url=url, table="fact_table", mode="append", properties=properties)