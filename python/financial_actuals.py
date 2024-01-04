#Import all required modules to connect to Odoo, process dataframes and import into Postgres database on latitude45.biz
import os
from dotenv import load_dotenv
import tempfile

import python_files.load_data as load_data #Imports functions defined in load_data.py in the sub directory python_files

import odoo_connect
import odoo_connect.data as odoo_data
from odoo_connect.explore import explore
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import numpy as np
import json
import datetime
from pandasql import sqldf
import base64
import requests
from datetime import date

#Merging strings into one by priority (from str4 to str1)
def merge_strings(str4, str3, str2, str1): 
    merged_string = str1
    if len(str2) > 0 :
        merged_string = str2
    if len(str3) > 0 :
        merged_string = str3
    if len(str4) > 0 :
        merged_string = str4
    
    return merged_string 


load_dotenv() #Reads the content of .env


#Step 1 - Read the Dimensions file and load in a dataframe 
file_url = ${CUSTOMER_FINANCIAL_FILES}/Dimensions.xlsx

dimensions = pd.read_excel(file_url, sheet_name=${CUSTOMER_FINANCIAL_MATCHING_ACCOUNTING_CODES},engine='openpyxl')
dimensions = dimensions.astype({'AccountNumberPCG':'int'})

print("Step 1 - Read the Dimensions file and load in a dataframe")
print(dimensions.head())

#Step 2 - Read the Forecast file and load in a dataframe
file_url =  ${CUSTOMER_FINANCIAL_FILES}/Forecast/forecast.xlsx

forecast = pd.read_excel(file_url, sheet_name='Compilation',engine='openpyxl')

print("Step 2 - Read the Forecast file and load in a dataframe")
print("--> Dataframe header")
print(forecast.head())

print('\n--> Unpivoting the Forecast compilation')

forecast = pd.melt(forecast, id_vars = ['ID', 'Statement', 'AccountNumberPCG', 'Description'], var_name = 'date', value_name = 'balance')

forecast['date'] = pd.to_datetime(forecast['date'], format='%Y-%m-%d')
forecast['balance'] = forecast['balance'].astype(float)
forecast['version'] = "Forecast"
forecast['name'] = "00-Forecast"

forecast = forecast.drop(columns={'Statement'})
forecast = forecast.set_index('AccountNumberPCG').join(dimensions.set_index('AccountNumberPCG'), rsuffix='_L1')

forecast = forecast.rename(columns={"Description": "account_id.name", "AccountNumberPCG": "account_id.code"})

#Step 3 - Read the Budget file and load in a dataframe
file_url =  ${CUSTOMER_FINANCIAL_FILES}/Budget/budget.xlsx

budget = pd.read_excel(file_url, sheet_name='Compilation',engine='openpyxl')

print("Step 3 - Read the Budget file and load in a dataframe")
print("--> Dataframe header")
print(budget.head())

print('\n--> Unpivoting the Budget compilation')

budget = pd.melt(budget, id_vars = ['ID', 'Statement', 'AccountNumberPCG', 'Description'], var_name = 'date', value_name = 'balance')

budget['date'] = pd.to_datetime(budget['date'], format='%Y-%m-%d')
budget['balance'] = budget['balance'].astype(float)
budget['version'] = "Budget"
budget['name'] = "00-Budget"

budget = budget.drop(columns={'Statement'})
budget = budget.set_index('AccountNumberPCG').join(dimensions.set_index('AccountNumberPCG'), rsuffix='_L1')

budget = budget.rename(columns={"Description": "account_id.name", "AccountNumberPCG": "account_id.code"})

concat = pd.concat([forecast, budget], ignore_index=True)
concat = concat.rename(columns={'Description_L1': 'Description'})

concat = concat.drop(columns={'Sorting key'})

concat.columns= concat.columns.str.lower()

concat['balance'].round(decimals = 2)

#Step 4 - Select the account.account.template environment and load the dataframe
odoo = env = odoo_connect.connect(url=${CUSTOMER_ODOO_LOCATION}, username=${CUSTOMER_ODOO_USER}, password=${CUSTOMER_ODOO_PASSWORD})

print("Step 4  - Select the account.account.template environment and load the dataframe")
so = env['account.account.template']
accounts = explore(so)
accounts = odoo_data.export_data(so, [], ['id', 'name', 'account_type', 'chart_template_id', 'code'])

accounts = pd.DataFrame(accounts)

#Promote first row as headers and remove first line
accounts.columns = accounts.iloc[0]
accounts = accounts[1:]

#5 out of 5 - Select the account.move.line environment and load the dataframe
print("Step 5 - Select the account.move.line environment and load the dataframe")
so = env['account.move.line']
journal_entries_details = explore(so)

#select specific fields from Odoo
journal_entries_details = odoo_data.export_data(so, [], ['id', 'date', 'name', 'account_id.id', 'account_id.name', 'account_id.code', 'debit', 'credit', 'balance'])

journal_entries_details = pd.DataFrame(journal_entries_details)

#Promote first row as headers and remove first line
journal_entries_details.columns = journal_entries_details.iloc[0]
journal_entries_details = journal_entries_details[1:]

#Truncate account numbers to look for a match with Plan Comptable Général (further down)
print('--> Truncating account numbers')
count = 1
for c in journal_entries_details['account_id.code'] :
   if isinstance (c, str) :
      account_5 = c[:5]
      account_4 = c[:4]
      account_3 = c[:3]
      account_2 = c[:2]
      journal_entries_details.loc[count,'account_5'] = int(account_5)
      journal_entries_details.loc[count,'account_4'] = int(account_4)
      journal_entries_details.loc[count,'account_3'] = int(account_3)
      journal_entries_details.loc[count,'account_2'] = int(account_2)
   count = count+1

#Truncated account numbers look for a match in the PCG previously loaded from the Dimensions.xlsx file
print('--> Look for a match in PCG loaded from Dimensions.xlsx')
journal_entries_details = journal_entries_details.set_index('account_2').join(dimensions.set_index('AccountNumberPCG'), rsuffix='_L1')
journal_entries_details = journal_entries_details.set_index('account_3').join(dimensions.set_index('AccountNumberPCG'), rsuffix='_L2')
journal_entries_details = journal_entries_details.set_index('account_4').join(dimensions.set_index('AccountNumberPCG'), rsuffix='_L3')
journal_entries_details = journal_entries_details.set_index('account_5').join(dimensions.set_index('AccountNumberPCG'), rsuffix='_L4')

print('--> Starting account assignments')

#For all Statements, Categories, Sub-Categories and Descriptions, merge strings with a priority on L4, L3, L2 then L1 statements
for row in journal_entries_details.itertuples():
   str4 = str(row.Statement_L4)
   str3 = str(row.Statement_L3)
   str2 = str(row.Statement_L2)
   str1 = str(row.Statement)
   merged_string = merge_strings(str4.replace("nan", ""), str3.replace("nan", ""), str2.replace("nan", ""), str1.replace("nan", ""))
   journal_entries_details.at[row.Index, 'statement'] =  merged_string  # store merged string in new column 'Merged'
   str4 = str(row.Category_L4)
   str3 = str(row.Category_L3)
   str2 = str(row.Category_L2)
   str1 = str(row.Category)
   merged_string = merge_strings(str4.replace("nan", ""), str3.replace("nan", ""), str2.replace("nan", ""), str1.replace("nan", ""))
   journal_entries_details.at[row.Index, 'category'] = merged_string  # store merged string in new column 'Merged'
   str4 = str(row.Sub_Category_L4)
   str3 = str(row.Sub_Category_L3)
   str2 = str(row.Sub_Category_L2)
   str1 = str(row.Sub_Category)
   merged_string = merge_strings(str4.replace("nan", ""), str3.replace("nan", ""), str2.replace("nan", ""), str1.replace("nan", ""))
   journal_entries_details.at[row.Index, 'sub_category'] = merged_string  # store merged string in new column 'Merged'
   str4 = str(row.Description_L4)
   str3 = str(row.Description_L3)
   str2 = str(row.Description_L2)
   str1 = str(row.Description)
   merged_string = merge_strings(str4.replace("nan", ""), str3.replace("nan", ""), str2.replace("nan", ""), str1.replace("nan", ""))
   journal_entries_details.at[row.Index, 'description'] = merged_string  # store merged string in new column 'Merged'

print('--> Done with account assignments')
print("Cleaning entries")

journal_entries_details['category'] = journal_entries_details['category'].str.replace("fici", "financi")
journal_entries_details['sub_category'] = journal_entries_details['sub_category'].str.replace("fici", "financi")

journal_entries_details['version'] = "Actuals"

journal_entries_details['balance'] = - journal_entries_details['balance']

#Drop some unused columns from the journal_entries_details dataframe
journal_entries_details = journal_entries_details.drop(columns={'Statement', 'Statement_L2', 'Statement_L3', 'Statement_L4', 'Category', 'Category_L2', 'Category_L3', 'Category_L4', 'Sub_Category', 'Sub_Category_L2', 'Sub_Category_L3', 'Sub_Category_L4', 'Description', 'Description_L2', 'Description_L3', 'Description_L4', 'Sorting key_L4', 'Sorting key_L3', 'Sorting key_L2', 'Sorting key'})

concat = pd.concat([journal_entries_details, concat], ignore_index=True)

concat = concat[concat.statement.notnull()]

#Combining the statements into Cash-Flow, Cumulative Debt  and  P&L type tables
print('Step 6 - Combining statements in a P&L like table')

BUDGET = concat.loc[concat['version'] == 'Budget']
BUDGET = BUDGET.rename(columns={'balance':'budget'})

forecast = forecast[forecast.date >= date.today().strftime('%Y-%m-%d')]

ACTUALS_FORECAST = concat.loc[(concat['version'] == 'Actuals') | (concat['version'] == 'Forecast')]
ACTUALS_FORECAST = ACTUALS_FORECAST.rename(columns = {'balance': 'actuals_forecast'})

FINAL_table = pd.concat([BUDGET, ACTUALS_FORECAST], ignore_index=True)
FINAL_table['date'] = pd.to_datetime(FINAL_table['date'], 'coerce')

print('Step 7 - Build CashFLow & Cumulative Debt tables')
CF_table = FINAL_table.loc[FINAL_table['statement'] == 'Trésorerie']
CD_table = FINAL_table.loc[FINAL_table['statement'] == 'Dette cumulée']
BS_table = FINAL_table.loc[FINAL_table['statement'] == 'Bilan']

print("--> Dataframe header")
print(CF_table)
print(CD_table)
print(BS_table)

forecast = forecast[forecast.date >= date.today().strftime('%Y-%m-%d')]

ACTUALS_FORECAST = concat.loc[(concat['version'] == 'Actuals') | (concat['version'] == 'Forecast')]
ACTUALS_FORECAST = ACTUALS_FORECAST.rename(columns = {'balance': 'actuals_forecast'})

FINAL_table = pd.concat([BUDGET, ACTUALS_FORECAST], ignore_index=True)
FINAL_table['date'] = pd.to_datetime(FINAL_table['date'], 'coerce')

print('Step 8 - Build P&L table')

PnL_table = FINAL_table.loc[FINAL_table['statement'] == 'Compte de résultats']

print("--> Dataframe header")
print(PnL_table)

#Create the connection to postgresql database on postgres.latitude45.biz server
engine = create_engine('postgresql+psycopg2://postgres:Evre57OyEiZwEbU!@localhost:55432/vocsens_financial_actuals')

#Load the dataframes into postgres
print("Step 9 - Load all dataframes in postgres database")
accounts.to_sql('vocsens_financial_actuals_accounts', con=engine, if_exists= 'replace', index=False, method = 'multi')
concat.to_sql('vocsens_financial_actuals_journal_entries_details', con=engine, if_exists= 'replace', index=False, method = 'multi')
PnL_table.to_sql('vocsens_PnL_table', con=engine, if_exists= 'replace', index=False, method = 'multi')
CF_table.to_sql('vocsens_CF_table', con=engine, if_exists= 'replace', index=False, method = 'multi')
CD_table.to_sql('vocsens_CD_table', con=engine, if_exists= 'replace', index=False, method = 'multi')
BS_table.to_sql('vocsens_BS_table', con=engine, if_exists= 'replace', index=False, method = 'multi')

print("Completed")
