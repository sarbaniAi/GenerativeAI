import streamlit as st
import re

# import the code that generates the answer
import platform
import os
from langchain import PromptTemplate
import pandas as pd
from langchain.llms import AzureOpenAI
import openai
from langchain import OpenAI, SQLDatabase, SQLDatabaseChain
from dotenv import load_dotenv

openai.api_key = "ea8490e7a70c49c99d7c2c2aae019e23"
openai.api_base = "https://veersaazureopenai.openai.azure.com/" # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
openai.api_type = 'azure'
openai.api_version = '2022-12-01' # this may change in the future
deployment_name='text-davinci-003'

load_dotenv()


#'evaluations','consent_forms','beds','patient_schedules','buildings',
#include_tables=['beds','building_locations','buildings','consent_forms','evaluations','group_leaders','group_session_attendances','group_session_leaders','group_session_topics','gs_group_session_occurrences','gs_group_sessions','kipu_appointment_engine_appointments','kipu_appointment_engine_appointments_scheduleables','kipu_appointment_engine_appointments_schedulers'     ,'kipu_appointment_engine_tele_health_meetings','kipu_appointment_engine_tele_health_users','ktb_locations','locations','locations_users','patient_contacts','patient_evaluations','patient_masters','patient_processes','patient_schedule_sessions','patient_schedules','patients','restricted_access_teams','roles','roles_users','rooms','schedule_items','schedules','site_settings','topics','users']
openai.api_key = os.getenv("OPENAI_API_KEY")
#paitint schdule , billing base detail, paitients, location

db2 = SQLDatabase.from_uri("postgresql+psycopg2://postgres:kausik75@localhost:5433/chatgpt",sample_rows_in_table_info=1, include_tables=['patients'])
db1 = SQLDatabase.from_uri("postgresql+psycopg2://postgres:kausik75@localhost:5433/chatgpt",sample_rows_in_table_info=1, include_tables=[ 'locations','billing_batch_details','schedules'])
db3 = SQLDatabase.from_uri("postgresql+psycopg2://postgres:kausik75@localhost:5433/chatgpt",sample_rows_in_table_info=1, include_tables=[ 'group_session_attendances','gs_group_sessions','group_session_leaders'])
db4 = SQLDatabase.from_uri("postgresql+psycopg2://postgres:kausik75@localhost:5433/chatgpt",sample_rows_in_table_info=1, include_tables=['patient_schedules','patient_schedule_sessions'])
db5 = SQLDatabase.from_uri("postgresql+psycopg2://postgres:kausik75@localhost:5433/chatgpt",sample_rows_in_table_info=1, include_tables=['consent_forms','evaluations','patient_evaluations'])

llm = AzureOpenAI(deployment_name=deployment_name,temperature=0.2)
_DEFAULT_TEMPLATE ="""Given an input question, first create a syntactically correct {dialect} query to run, then look at the results of the query and return the answer as you are a healthcare consultant try to create answer in bullet points when "," is involved in query result in intermediate steps,if not answer in simple English sentences.
Use the following format:

SQLResult: "Result of the SQLQuery"
Answer: "Final answer here"

Only use the following tables:

{table_info}

Question: {input}"""

PROMPT = PromptTemplate(
    input_variables=["input", "table_info", "dialect"], template=_DEFAULT_TEMPLATE
)
# FORMAT_RESPONSE_PROMPT = """
# Given the following response:

# {response}

# Format the response in a structured manner and if count is included in query display result in structured tabular format ,otherwise display the result normally and return the formatted response.

# Formatted Response:
# """



db_chain1 = SQLDatabaseChain(llm=llm, database=db1, prompt=PROMPT, verbose=False, return_intermediate_steps=True)
db_chain2 = SQLDatabaseChain(llm=llm, database=db2, prompt=PROMPT, verbose=False, return_intermediate_steps=True)
db_chain3 = SQLDatabaseChain(llm=llm, database=db3, prompt=PROMPT, verbose=False, return_intermediate_steps=True)
db_chain4 = SQLDatabaseChain(llm=llm, database=db4, prompt=PROMPT, verbose=False, return_intermediate_steps=True)
db_chain5 = SQLDatabaseChain(llm=llm, database=db5, prompt=PROMPT, verbose=False, return_intermediate_steps=True)

# create a Streamlit web app
def app():
    # set the title of the web app
    st.title("VGPT_NEW")

    # get the user input question
    question = st.text_input("Enter your question here:")

    # if the user submits the question
    if st.button("Submit"):
        # generate the SQL query and get the answer
        try:
            print('started ------------db1--------------')
            output = db_chain1(f"{question} ")
            print('------------db1--------------')
            print(output)
            response = output['result']
            # formatted_response = openai.Completion.create(
            #     engine="text-davinci-002",
            #     prompt=FORMAT_RESPONSE_PROMPT.format(response=response),
            #     max_tokens=200,
            #     temperature=0.5,
            #     n=1,
            #     stop=None,
            #     timeout=10,
            # ).choices[0].text.strip()

            # display the formatted response
            st.write(response)
        except Exception as e:
            print(e)
            try:
                print('started ------------db2--------------')
                output = db_chain2(f"{question} from database tables")
                print('------------db2--------------')
                print(output)
                response = output['result']
                # formatted_response = openai.Completion.create(
                #     engine="text-davinci-002",
                #     prompt=FORMAT_RESPONSE_PROMPT.format(response=response),
                #     max_tokens=200,
                #     temperature=0.5,
                #     n=1,
                #     stop=None,
                #     timeout=10,
                # ).choices[0].text.strip()

                # display the formatted response
                st.write(response)
                # if an error occurs, display a general answer generated by GPT-3
            except :
                try:
                    print('started ------------db3--------------')
                    output = db_chain3(f"{question} from database tables")
                    print('------------db3--------------')
                    print(output)
                    response = output['result']
                    # formatted_response = openai.Completion.create(
                    #     engine="text-davinci-002",
                    #     prompt=FORMAT_RESPONSE_PROMPT.format(response=response),
                    #     max_tokens=200,
                    #     temperature=0.5,
                    #     n=1,
                    #     stop=None,
                    #     timeout=10,
                    # ).choices[0].text.strip()

                    # display the formatted response
                    st.write(response)
                    # if an error occurs, display a general answer generated by GPT-3
                except:
                    try:
                        print('started ------------db4--------------')
                        output = db_chain4(f"{question} from database tables")
                        print('------------db4--------------')
                        print(output)
                        response = output['result']
                        # formatted_response = openai.Completion.create(
                        #     engine="text-davinci-002",
                        #     prompt=FORMAT_RESPONSE_PROMPT.format(response=response),
                        #     max_tokens=200,
                        #     temperature=0.5,
                        #     n=1,
                        #     stop=None,
                        #     timeout=10,
                        # ).choices[0].text.strip()

                        # display the formatted response
                        st.write(response)
                        # if an error occurs, display a general answer generated by GPT-3
                    except Exception as e:
                        try:
                            print('started ------------db5--------------')
                            output = db_chain5(f"{question} from database tables")
                            print('------------db5--------------')
                            print(output)
                            response = output['result']
                            # formatted_response = openai.Completion.create(
                            #     engine="text-davinci-002",
                            #     prompt=FORMAT_RESPONSE_PROMPT.format(response=response),
                            #     max_tokens=200,
                            #     temperature=0.5,
                            #     n=1,
                            #     stop=None,
                            #     timeout=10,
                            # ).choices[0].text.strip()

                            # display the formatted response
                            st.write(response)
                            # if an error occurs, display a general answer generated by GPT-3
                        except Exception as e:
                            print(f"The code is in exception {e}")
                            response = openai.Completion.create(
                                model='text-davinci-003',
                                engine= deployment_name,
                                prompt=question,
                                temperature=0.0,
                                max_tokens=60,
                                top_p=1,
                                frequency_penalty=0,
                                presence_penalty=0.6,
                            )
                            answer = response.choices[0].text.strip()
                            st.write(answer)

if __name__ == "__main__":
    app()