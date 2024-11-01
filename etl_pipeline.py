# Import the neccessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import psycopg2
import os

# Set java home 
os.environ['JAVA_HOME'] = 'C:/java8'

# Initialize the spark session

builder = SparkSession.builder.appName("Nuga Bank ETL")
builder.config('spark.jars', 'postgresql-42.7.4.jar')
builder.config("spark.driver.memory", "4g")         # Set driver memory to 4 GB.
builder.config("spark.executor.memory", "4g")       # Set executor memory to 4 GB.
builder.config("spark.executor.memoryOverhead", "1g")  # Set additional executor memory overhead.
spark = builder.getOrCreate()

# Load the data

df = spark.read.csv(r'dataset\raw_data\nuga_bank_transactions.csv', header=True, inferSchema=True)

# Data Cleaninig and transformation
#fill up the missing values

df_clean = df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address':'Unknown',
    'Customer_City':'Unknown',
    'Customer_State':'Unknown',
    'Customer_Country':'Unknown',
    'Company':'Unknown',
    'Job_Title':'Unknown',
    'Phone_Number':'Unknown',
    'Credit_Card_Number':0,
    'IBAN':'Unknown',
    'Currency_Code':'Unknown',
    'Email':'Unknown',
    'Random_Number':0.0,
    'Category':'Unknown',
    'Group':'Unknown',
    'Is_Active':'Unknown',
    'Marital_Status':'Unknown',
    'Description':'Unknown',
    'Gender':'Unknown',
    })

# drop the null values

df_clean = df.na.drop(subset='Last_Updated')

# Data Transformation to 2NF
# Transcation Table

transaction = df_clean.select('Transaction_Date','Amount','Transaction_Type',) \
      .withColumn('Transaction_id',monotonically_increasing_id())\
            .select('Transaction_id','Transaction_Date','Amount','Transaction_Type')

customer = df_clean.select('Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country')\
      .withColumn('Customer_id',monotonically_increasing_id())\
            .select('Customer_id','Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country')

employee = df_clean.select('Company','Job_Title','Email','Phone_Number','Gender','Marital_Status')\
      .withColumn('Employee_ID',monotonically_increasing_id())\
            .select('Employee_ID','Company','Job_Title','Email','Phone_Number','Gender','Marital_Status')

nuga_fact_table = df_clean.join(transaction,['Transaction_Date','Amount','Transaction_Type'], 'inner')\
      .join(customer,['Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country'],'inner')\
            .join(employee,['Company','Job_Title','Email','Phone_Number','Gender','Marital_Status'], 'inner')\
                  .select('Transaction_id','Customer_id','Employee_ID','Credit_Card_Number','IBAN','Currency_Code','Random_Number','Category','Group','Is_Active','Description')

# Data Loading
# Set up database connection
def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'nuga_bank_pyspark',
        user = 'postgres',
        password = 'password'
    )
    return connection

# connect to sql database

conn = get_db_connection()

# Create a function to create tables

def create_table():
    cursor = conn.cursor()
    create_table_query = ''' 
                              DROP TABLE IF EXISTS customer;
                              DROP TABLE IF EXISTS transaction;
                              DROP TABLE IF EXISTS employee;
                              DROP TABLE IF EXISTS nuga_fact_table;

                              CREATE TABLE customer(
                              Customer_id BIGINT,
                              Customer_Name VARCHAR(10000),
                              Customer_Address VARCHAR(10000),
                              Customer_City VARCHAR(10000),
                              Customer_State VARCHAR(10000),
                              Customer_Country VARCHAR(10000)
                              );
                              
                              CREATE TABLE transaction(
                              Transaction_id BIGINT,
                              Transaction_Date DATE,
                              Amount FLOAT,
                              Transaction_Type VARCHAR(1000)
                              );

                              CREATE TABLE employee(
                              Employee_ID BIGINT,
                              Company VARCHAR(1000),
                              Job_Title VARCHAR(1000),
                              Email VARCHAR(1000),
                              Phone_Number VARCHAR(1000),
                              Gender VARCHAR(1000),
                              Marital_Status VARCHAR(1000)
                              );
                              
                              CREATE TABLE nuga_fact_table(
                              Transaction_id BIGINT,
                              Customer_id BIGINT,
                              Employee_ID BIGINT,
                              Credit_Card_Number BIGINT,
                              IBAN VARCHAR(1000),
                              Currency_Code VARCHAR(100),
                              Random_Number FLOAT,
                              Category VARCHAR(1000),
                              "Group" VARCHAR(1000),
                              Is_Active VARCHAR(1000),
                              Description VARCHAR(1000)
                              );
                        '''
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()
    cursor.close()

# Call the function to create the table
create_table()

### Appending Spark DataFrame to PostgreSQL Table Using JDBC

# Define the JDBC URL to connect to the PostgreSQL database
url = "jdbc:postgresql://localhost:5432/nuga_bank_pyspark"

# Set the connection properties including username, password, and JDBC driver
properties = {
    "user" : "postgres",
    "password":"password",
    "driver":"org.postgresql.Driver"  # PostgreSQL JDBC driver
}
### Write the Spark DataFrame to postgresSQL database
customer.write.jdbc(url=url, table="customer", mode="append", properties=properties)
employee.write.jdbc(url=url, table="employee", mode="append", properties=properties)
transaction.write.jdbc(url=url, table="transaction", mode="append", properties=properties)
nuga_fact_table.write.jdbc(url=url, table="nuga_fact_table", mode="append", properties=properties)

print('Databese table created and loaded successfully')