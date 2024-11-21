# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook will 
# MAGIC
# MAGIC - Create the required Agent tools - functions
# MAGIC - Create the sample data & upload in Delta Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Create UC-Functions to access the yahoo finance API
# MAGIC
# MAGIC Get access to :
# MAGIC https://www.financeapi.net/
# MAGIC
# MAGIC Create your own API-KEY ( free key will have limitations, please check the dashboard, limk below)
# MAGIC
# MAGIC Access to Yahoofinace dashboard : 
# MAGIC  https://www.financeapi.net/dashboard
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Yahoofinance alternative 
# MAGIC
# MAGIC https://site.financialmodelingprep.com/developer/docs#balance-sheet-statements-financial-statements
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC I have used one specfic researcher "Argus Research" to limit the number of tokens

# COMMAND ----------

# Drop the existing function if it exists to avoid conflicts
spark.sql('DROP FUNCTION IF EXISTS sarbanimaiti_catalog.agent_demo.finance_insight_api;')

# Create or replace the Unity Catalog function for financial insights
spark.sql('''
CREATE OR REPLACE FUNCTION sarbanimaiti_catalog.agent_demo.finance_insight_api (query STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Returns financial insights for a given ticker symbol.'
AS
$$
try:
    # Import requests module to make HTTP requests
    import requests
    # Define the API endpoint for financial insights
    url = "https://yfapi.net/ws/insights/v1/finance/insights"

    # Set the query parameter with the symbol provided to the function
    params = {"symbol": query}

    # Include your API key in the request headers for authentication
    headers = {
        'x-api-key': "<your own api-key>"
    }

    # Make a GET request to the API
    response = requests.request("GET", url, headers=headers, params=params)

    # Initialize an empty list to hold summaries
    summaries = []
    # Parse the JSON response
    data = response.json()
    # Control variable to ensure only one summary is added
    cntrol = 1

    # Iterate through the reports in the response
    for report in data['finance']['result']['reports']:
        # Check if the report is from Argus Research and control is 1
        if report['provider'] == "Argus Research" and cntrol ==1:
            # Append the summary to the summaries list
            summaries.append(report['summary'])
            # Update control to prevent adding more summaries
            cntrol = cntrol + 1

    # Return the list of summaries
    return summaries
except Exception as e:
    # Return an error message if an exception occurs
    return "Error calling YouTube Search API: {{e}}".format(e=str(e))
$$;
''')

# COMMAND ----------

# Test the finance_insight_api function with 'AAPL' as the parameter and collect the result
result = spark.sql("SELECT sarbanimaiti_catalog.agent_demo.finance_insight_api('AAPL')").collect()

# COMMAND ----------

result

# COMMAND ----------

# # Drop the existing function to avoid conflicts
# spark.sql('DROP FUNCTION IF EXISTS sarbanimaiti_catalog.finance_stock_quote.finance_api;')

# Define a new function in the Unity Catalog
spark.sql('''
CREATE OR REPLACE FUNCTION sarbanimaiti_catalog.agent_demo.finance_stock_quote (query STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Returns financial summary of ticker.'
AS
$$
try:
    # Import requests to enable HTTP calls
    import requests
    # API endpoint for fetching finance quotes
    url = "https://yfapi.net/v6/finance/quote"
    # Parameters for the API call, specifying the stock symbols
    params = {"symbols": query}
    # Headers for the API call, including the API key for authentication
    headers = {
        'x-api-key': "<your own api-key>"
    }
    # Execute the GET request with specified URL, headers, and parameters
    response = requests.request("GET", url, headers=headers, params=params)
    # Return the JSON response from the API
    return response.json()
except Exception as e:
    # Return an error message if an exception occurs
    return "Error calling YouTube Search API: {{e}}".format(e=str(e))
$$;
''')

# COMMAND ----------

# Test the SQL function to get the real time price , financial summary for the ticker 'AAPL' and collect the result
result = spark.sql("SELECT sarbanimaiti_catalog.agent_demo.finance_stock_quote('AAPL')").collect()

# COMMAND ----------

result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the stock historical price tables
# MAGIC
# MAGIC ##### Upload the stock historical files from "sample_stock_hist_files" folder to your Unity catalog Volume. 
# MAGIC In real case these files can be sourced in various ways.
# MAGIC
# MAGIC ##### Then  run the below code to :
# MAGIC - Merge all the files in one spark data frame
# MAGIC - Then upload the themerged file in spark delta table which will be used in UC function (Agent Tool)

# COMMAND ----------

from pyspark.sql.functions import input_file_name, col, regexp_extract

# Function to rename columns to remove spaces and special characters
def rename_columns(df):
    return df.select([col(c).alias(c.replace(" ", "_").replace("Adj Close", "Adj_Close")) for c in df.columns])

# Load CSV files and add a column with the file name, then rename columns
tsla_df = rename_columns(
    spark.read.option("header", "true")
    .csv("/Volumes/sarbanimaiti_catalog/agent_demo/stocks_historical_price/stock_hist_price/TSLA_prices.csv")
    .withColumn("stock_name", regexp_extract(input_file_name(), r'([^/]+)(?=\.)', 1).substr(0, 4))
    
)

aapl_df = rename_columns(
    spark.read.option("header", "true")
    .csv("/Volumes/sarbanimaiti_catalog/agent_demo/stocks_historical_price/stock_hist_price/AAPL_prices.csv")
    .withColumn("stock_name", regexp_extract(input_file_name(), r'([^/]+)(?=\.)', 1).substr(0, 4))
)

googl_df = rename_columns(
    spark.read.option("header", "true")
    .csv("/Volumes/sarbanimaiti_catalog/agent_demo/stocks_historical_price/stock_hist_price/GOOGL_prices.csv")
    .withColumn("stock_name", regexp_extract(input_file_name(), r'([^/]+)(?=\.)', 1).substr(0, 4))
)

jnj_df = rename_columns(
    spark.read.option("header", "true")
    .csv("/Volumes/sarbanimaiti_catalog/agent_demo/stocks_historical_price/stock_hist_price/JNJ_prices.csv")
    .withColumn("stock_name", regexp_extract(input_file_name(), r'([^/]+)(?=\.)', 1).substr(0, 3))
)

jpm_df = rename_columns(
    spark.read.option("header", "true")
    .csv("/Volumes/sarbanimaiti_catalog/agent_demo/stocks_historical_price/stock_hist_price/JPM_prices.csv")
    .withColumn("stock_name", regexp_extract(input_file_name(), r'([^/]+)(?=\.)', 1).substr(0, 3))
)

amzn_df = rename_columns(
    spark.read.option("header", "true")
    .csv("/Volumes/sarbanimaiti_catalog/agent_demo/stocks_historical_price/stock_hist_price/AMZN_prices.csv")
    .withColumn("stock_name", regexp_extract(input_file_name(), r'([^/]+)(?=\.)', 1).substr(0, 4))
)


# Union all dataframes
merged_df = tsla_df.union(aapl_df).union(googl_df).union(jnj_df).union(jpm_df).union(amzn_df)

# Write to a Delta table
merged_df.write.format("delta").mode("overwrite").saveAsTable("sarbanimaiti_catalog.agent_demo.merged_stock_prices")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verify the table 
# MAGIC SELECT DISTINCT stock_name FROM sarbanimaiti_catalog.agent_demo.merged_stock_prices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Agent Tool : Extract stock closing price from history table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Drop the existing function to avoid conflicts
# MAGIC spark.sql('DROP FUNCTION IF EXISTS sarbanimaiti_catalog.agent_demo.get_historical_closing_price;')
# MAGIC
# MAGIC -- This function takes stock ticker and duration in terms of year as input, refer the corresponding stock historical price table and fetch the historical closing price & volume of the stock for that duration. 
# MAGIC -- Duration in year  eg : '3'
# MAGIC CREATE OR REPLACE FUNCTION sarbanimaiti_catalog.agent_demo.get_historical_closing_price (
# MAGIC   stock_ticker STRING COMMENT 'Stock ticker will be passed in the query',
# MAGIC   yr_duration  BIGINT COMMENT 'Duration for which stock volume and closing price will be extracted'
# MAGIC   
# MAGIC )
# MAGIC returns table(Date DATE , Close FLOAT, Volume INT)
# MAGIC return
# MAGIC (
# MAGIC WITH FilteredStocks AS (
# MAGIC   SELECT
# MAGIC     Date,
# MAGIC     Close,
# MAGIC     Volume,
# MAGIC     stock_name
# MAGIC   FROM
# MAGIC     sarbanimaiti_catalog.agent_demo.merged_stock_prices
# MAGIC   WHERE
# MAGIC     stock_name = stock_ticker
# MAGIC     AND datediff(CURRENT_DATE(), Date) <= 365 * yr_duration
# MAGIC )
# MAGIC SELECT
# MAGIC   Date,
# MAGIC   Close,
# MAGIC   Volume
# MAGIC FROM
# MAGIC   FilteredStocks
# MAGIC ORDER BY
# MAGIC   Date ASC
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test the tool 

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM sarbanimaiti_catalog.agent_demo.get_historical_closing_price('JPM', 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create table to load customer investment preference data
# MAGIC
# MAGIC Stesp:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

import pandas as pd
import random
import json
from faker import Faker

# Initialize Faker for generating synthetic data
fake = Faker()

# Define ranges and options for generating data
income_ranges = ["low", "medium", "high"]
risk_tolerances = ["low", "medium", "high"]
investment_horizons = ["short-term", "medium-term", "long-term"]
investment_goals = ["retirement", "wealth accumulation", "education", "savings"]
industries = ["tech", "healthcare", "energy", "finance", "consumer goods"]
stock_types = ["growth", "dividend", "blue-chip", "value", "penny"]
trading_frequencies = ["daily", "weekly", "monthly"]
holding_periods = ["days", "weeks", "months", "years"]
special_requirements_options = ["ESG-focused investments", "no tobacco stocks", "", "", ""]

# Generate synthetic data
data = []
num_records = 100  # Number of records to generate

for _ in range(num_records):
    customer_id = fake.random_int(min=1000, max=9999)
    age = fake.random_int(min=18, max=75)
    gender = fake.random_element(elements=("Male", "Female"))
    income_range = fake.random_element(elements=income_ranges)
    risk_tolerance = fake.random_element(elements=risk_tolerances)
    investment_horizon = fake.random_element(elements=investment_horizons)
    investment_goal = fake.random_element(elements=investment_goals)
    preferred_industries = random.sample(industries, k=random.randint(1, 3))
    preferred_stock_type = fake.random_element(elements=stock_types)
    average_investment_amount = round(random.uniform(1000, 100000), 2)
    trading_frequency = fake.random_element(elements=trading_frequencies)
    holding_period = fake.random_element(elements=holding_periods)
    historical_performance = json.dumps({
        "avg_return": round(random.uniform(-10, 20), 2),
        "volatility": round(random.uniform(0.5, 3.0), 2)
    })
    special_requirements = fake.random_element(elements=special_requirements_options)
    
    # Add record to data list
    data.append([
        customer_id, age, gender, income_range, risk_tolerance, investment_horizon, 
        investment_goal, preferred_industries, preferred_stock_type, 
        average_investment_amount, trading_frequency, holding_period, 
        historical_performance, special_requirements
    ])

# Create DataFrame
columns = [
    "customer_id", "age", "gender", "income_range", "risk_tolerance", "investment_horizon",
    "investment_goals", "preferred_industries", "preferred_stock_types", 
    "average_investment_amount", "trading_frequency", "holding_period", 
    "historical_performance", "special_requirements"
]
df_synthetic = pd.DataFrame(data, columns=columns)

# Save to CSV if needed
#df_synthetic.to_csv("customer_stock_preferences_synthetic.csv", index=False)

print("Synthetic data generated and saved to 'customer_stock_preferences_synthetic.csv'.")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Upload Customer Stock Preferences data to spark delta table 

# COMMAND ----------

df_spark = spark.createDataFrame(df_synthetic)
df_spark.write.format("delta").mode("overwrite").saveAsTable("sarbanimaiti_catalog.agent_demo.customer_investment_preferences")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Create Agent Tool : Extract Customer investment preference

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Again check that it exists
# MAGIC DROP FUNCTION IF EXISTS sarbanimaiti_catalog.agent_demo.customer_investment_preferences;
# MAGIC
# MAGIC -- This function takes stock ticker and duration in terms of year as input, refer the corresponding stock historical price table and fetch the historical closing price & volume of the stock for that duration. 
# MAGIC -- Duration in year  eg : '3'
# MAGIC CREATE OR REPLACE FUNCTION sarbanimaiti_catalog.agent_demo.customer_investment_preferences (
# MAGIC   customerID BIGINT COMMENT 'Customer ID'
# MAGIC   
# MAGIC )
# MAGIC returns table(
# MAGIC   customer_id BIGINT, 
# MAGIC   age BIGINT,
# MAGIC   gender STRING, 
# MAGIC   income_range STRING, 
# MAGIC   risk_tolerance STRING, 
# MAGIC   investment_horizon STRING, 
# MAGIC   investment_goals STRING, 
# MAGIC   preferred_industries STRING, 
# MAGIC   preferred_stock_types STRING, 
# MAGIC   average_investment_amount DOUBLE, 
# MAGIC   trading_frequency BIGINT, 
# MAGIC   holding_period BIGINT, 
# MAGIC   historical_performance DOUBLE, 
# MAGIC   special_requirements STRING)
# MAGIC return
# MAGIC (
# MAGIC WITH CustomerPreference  AS (
# MAGIC   SELECT
# MAGIC     customer_id, 
# MAGIC     age,
# MAGIC     gender, 
# MAGIC     income_range, 
# MAGIC     risk_tolerance, 
# MAGIC     investment_horizon, 
# MAGIC     investment_goals, 
# MAGIC     preferred_industries, 
# MAGIC     preferred_stock_types, 
# MAGIC     average_investment_amount, 
# MAGIC     trading_frequency, 
# MAGIC     holding_period, 
# MAGIC     historical_performance, 
# MAGIC     special_requirements
# MAGIC   FROM
# MAGIC     sarbanimaiti_catalog.agent_demo.customer_investment_preferences
# MAGIC   WHERE
# MAGIC     customer_id = customerID )
# MAGIC   select * from CustomerPreference 
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --test function
# MAGIC select * from sarbanimaiti_catalog.agent_demo.customer_investment_preferences(3584)
# MAGIC

# COMMAND ----------


