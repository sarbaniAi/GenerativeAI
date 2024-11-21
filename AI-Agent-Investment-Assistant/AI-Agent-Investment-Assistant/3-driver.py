# Databricks notebook source
# MAGIC %md
# MAGIC # Driver notebook
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI Playground export. We generated three notebooks in the same folder:
# MAGIC - [agent]($./agent): contains the code to build the agent.
# MAGIC - [config.yml]($./config.yml): contains the configurations.
# MAGIC - [**driver**]($./driver): logs, evaluate, registers, and deploys the agent.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to deploy the agent defined in the [agent]($./agent) notebook. The notebook does the following:
# MAGIC 1. Logs the agent to MLflow
# MAGIC 2. Registers the agent to Unity Catalog
# MAGIC 3. Deploys the agent to a Model Serving endpoint
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.
# MAGIC - Review the contents of [config.yml]($./config.yml) as it defines the tools available to your agent and the LLM endpoint.
# MAGIC - Review and run the [agent]($./agent) notebook in this folder to view the agent's code, iterate on the code, and test outputs.
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See docs ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)) for details

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [agent]($./agent) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

import os
import mlflow
from mlflow.models import ModelConfig
from mlflow.models.signature import ModelSignature
from mlflow.models.rag_signatures import ChatCompletionRequest, ChatCompletionResponse
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from databricks.sdk import WorkspaceClient

# Initialize WorkspaceClient
w = WorkspaceClient()

# Load model configuration
config = ModelConfig(development_config="config.yml")

# Define resources for the model
resources = [DatabricksServingEndpoint(endpoint_name=config.get("llm_endpoint"))]

# Expand user-defined functions (UDFs) if specified in the config
uc_functions_to_expand = config.get("tools").get("uc_functions")
for func in uc_functions_to_expand:
    if func.endswith("*"):
        catalog, schema, _ = func.split(".")
        expanded_functions = list(w.functions.list(catalog_name=catalog, schema_name=schema))
        for expanded_function in expanded_functions:
            resources.append(DatabricksFunction(function_name=expanded_function.full_name))
    else:
        resources.append(DatabricksFunction(function_name=func))

# Define model signature
signature = ModelSignature(ChatCompletionRequest(), ChatCompletionResponse())

# Define input example for the model
input_example = {"messages": [{"role": "user", "content": "should I invest in tesla stocks?"}]}

# Log the model to MLflow
with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        "agent",
        python_model=os.path.join(os.getcwd(), "agent"),
        signature=signature,
        input_example=input_example,
        model_config="config.yml",
        resources=resources,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.

# COMMAND ----------

import pandas as pd

eval_examples = [
    {
        "request":  "I am reviewing my investment portfolio. What should I do with Tesla stock?",

        "expected_response": "Hold Justification: \
Based on the analysis of the provided data, I recommend holding onto Tesla stock (TSLA). Here's a summary of the insights: \
Current Stock Price: As of the latest data, the regular market price of TSLA is $338.74 (finance_stock_quote). \
Stock Summary: The finance_insight_api provides a neutral summary of the market, discussing the presidential cycle and its impact on stocks. While it doesn't specifically mention Tesla, it suggests that the market might be in a period of uncertainty. \
Historical Closing Price and Volume: The get_historical_closing_price function provides data for the last year. Tesla's stock price has fluctuated, with a recent downturn in January 2024. However, it's essential to consider the overall trend and not just the short-term fluctuations. \
Given the insights, I recommend holding onto Tesla stock for the following reasons: \
The current stock price is around $338.74, which is relatively stable compared to the historical data. \
The presidential cycle analysis suggests that the market might be in a period of uncertainty, but it's essential to consider the long-term trend rather than short-term fluctuations.\
Tesla's stock price has fluctuated in the past, but it has generally shown resilience and growth. \
Its essential to note that this recommendation is based on the provided data and might not reflect the current market situation.It's always recommended to consult multiple sources and consider your own risk appetite before making investment decisions."
    }
    ,
    {
        "request": "What should be the choice for Apple stocks for customer id 1264?",
      
        "expected_response": "Hold \
Justification: \
Based on the analysis of the provided data, I recommend holding onto Apple stock (AAPL) for customer ID 1264. Here's a summary of the insights: \
Current Stock Price: As of the latest data, the regular market price of AAPL is $228.02 (finance_stock_quote). \
Customer Investment Preferences: The customer has a high-risk tolerance and a long-term investment horizon, with a focus on savings (customer_investment_preferences). \
Historical Closing Price and Volume: The get_historical_closing_price function provides data for the last year. Apple's stock price has fluctuated, with a recent downtrend in January 2024. However, it's essential to consider the overall trend and not just the short-term fluctuations. \
Given the insights, I recommend holding onto Apple stock for the following reasons: \
The customer high-risk tolerance and long-term investment horizon suggest that they can withstand market fluctuations and are likely looking for long-term growth. \
Apple's stock price has historically been stable, with a recent downtrend that may present a buying opportunity. \
The customer's focus on savings suggests that they may be looking for dividend-paying stocks, and Apple has a history of paying dividends. \
It's essential to note that this recommendation is based on the provided data and might not reflect the current market situation. It's always recommended to consult multiple sources and consider your own risk appetite before making investment decisions."
    },
    {
        "request": "I have 1000 Google stocks. Should I hold them?",
            
        "expected_response": "Hold \
Justification: \
Based on the analysis of the provided data, I recommend holding onto Google stock (GOOGL). Here's a summary of the insights: \
Current Stock Price: As of the latest data, the regular market price of GOOGL is $175.3 (finance_stock_quote). \
Historical Closing Price and Volume: The get_historical_closing_price function provides data for the last year, showing a steady increase in stock price from $136.25 on November 20, 2023, to $175.3 on the current date. \
Average Analyst Rating: The average analyst rating for GOOGL is \"1.6 - Buy\", indicating a positive outlook for the stock. \
Given the insights, I recommend holding onto Google stock for the following reasons: \
The current stock price is relatively stable, with a recent increase of 1.63% (finance_stock_quote). \
The historical data shows a steady increase in stock price over the last year, indicating a positive trend. \
The average analyst rating is positive, indicating a potential for future growth. \
Please note that this recommendation is based on the provided data and might not reflect the current market situation. It's always recommended to consult multiple sources and consider your own risk appetite before making investment decisions."
    }
     
]

eval_dataset = pd.DataFrame(eval_examples)
display(eval_dataset)

# COMMAND ----------

import mlflow
import pandas as pd

# Start an MLflow run using the provided run ID
with mlflow.start_run(run_id=logged_agent_info.run_id):
    # Evaluate the model using the specified run ID and evaluation dataset
    eval_results = mlflow.evaluate(
        f"runs:/{logged_agent_info.run_id}/agent",  # Path to the logged model artifact
        data=eval_dataset,  # Evaluation dataset
        model_type="databricks-agent",  # Specify the model type for evaluation
    )

# Display the evaluation results
display(eval_results.tables['eval_results'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# Define the catalog, schema, and model name for your UC model
catalog = "sarbanimaiti_catalog"
schema = "agent_demo"
model_name = "investment_assistant_agent_model"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# Register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents

# Deploy the registered model to the review app and a model serving endpoint
# UC_MODEL_NAME: The fully qualified name of the model in Unity Catalog
# uc_registered_model_info.version: The version of the model to deploy
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version)

# COMMAND ----------

