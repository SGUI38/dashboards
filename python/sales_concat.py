#Import all required modules to connect to Odoo, process dataframes and import into Postgres database on latitude45.biz
import odoo_connect
import odoo_connect.data as odoo_data
from odoo_connect.explore import explore
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import numpy as np
import requests
from requests.structures import CaseInsensitiveDict
import json
import sys
from pandasql import sqldf
import country_converter as coco

def replaceNone(data_dict,v,rv):
    for key in data_dict.keys():
        if data_dict[key] == v:
            data_dict[key] = rv
        elif type(data_dict[key]) is dict:
            replaceNone(data_dict[key],v,rv)

def flatten_list(lst):

    # Remove all occurences of "NaN" from the list
    lst = [x for x in lst if str(x) != 'nan']

    # Join the list values into a string with a space between each value
    result = ' '.join(str(v) for v in lst)

    return result

#Define the string to connect to VOCSens odoo
odoo = env = odoo_connect.connect(url='https://vocsens.odoo.com/', username='sam@guilaume.fr', password='45d0c0805bbc6c8fb89674121d0a8545637b285e')

#Select the crm.lead environment and load the dataframe
so = env['crm.lead']
opportunities = pd.DataFrame(odoo_data.export_data(so, [('type', '=', 'opportunity')], ['id', 'date_deadline', 'date_closed', 'priority', 'display_name', 'stage_id.name', 'probability', 'expected_revenue', 'won_status', 'partner_name', 'city', 'country_id.code', 'email_from', 'user_id.name',  'create_date', 'tag_ids', 'partner_id.id']))

#Generate a combined column using ID and Opportunity name
opportunities['Unique_Opportunity'] = opportunities[0].astype(str) + ' - ' + opportunities[4]

#Promote first row as headers and remove first line
opportunities.columns = opportunities.iloc[0]
opportunities = opportunities[1:]

opportunities['opportunity_UID'] = opportunities.reset_index().index

#Explode the tag_ids field and create as many rows
opportunities = opportunities.explode('tag_ids')

#Remove the column ID, not used anymore
opportunities = opportunities.drop(columns=['id'])

#Convert the following date columns from text strings to date formats. will be used to generate the postgresql database
opportunities['date_deadline'] = pd.to_datetime(opportunities['date_deadline'], 'coerce')
opportunities['date_closed'] = pd.to_datetime(opportunities['date_closed'], 'coerce')
opportunities['create_date'] = pd.to_datetime(opportunities['create_date'], 'coerce')

#Calculate the Net revenue as a factored expected revenue with probabiltiy
opportunities['Net Revenue'] = opportunities['expected_revenue'] * opportunities['probability'] / 100

print(opportunities[opportunities['display_name'].str.contains('DOW')])

#Select the crm/tag environment and load the dataframe
so = env['crm.tag']
tags = pd.DataFrame(odoo_data.export_data(so, [], ['id', 'display_name']))

#Promote first row as headers and remove first line
tags.columns = tags.iloc[0]
tags = tags[1:]

#Select the res.partner environment and load the dataframe
so = env['res.partner']
partner_industry = pd.DataFrame(odoo_data.export_data(so, [], ['id', 'industry_id']))

#Promote first row as headers and remove first line
partner_industry.columns = partner_industry.iloc[0]
partner_industry = partner_industry[1:]

#Select the res.partner.industry environment and load the dataframe
so = env['res.partner.industry']
industries = pd.DataFrame(odoo_data.export_data(so, [], ['id', 'name']))

#Promote first row as headers and remove first line
industries.columns = industries.iloc[0]
industries = industries[1:]

#Merge the partner_industry dataframe with the industry_id coming from the industries dataframe
partner_industry = partner_industry.merge(industries, left_on = 'industry_id', right_on = 'id')

#Merge the opportunities dataframe with the industry name coming from the partner_industry dataframe
opportunities = opportunities.merge(partner_industry, how='left', left_on = 'partner_id.id', right_on = 'id_x')

#Merge the opportunities dataframe with the tag name coming from the tags dataframe
opportunities = opportunities.merge(tags, how = 'left', left_on = 'tag_ids', right_on = 'id')

#Remove some unused columns
opportunities = opportunities.drop(columns = ['id_x', 'id_y', 'industry_id', 'id', 'partner_id.id', 'tag_ids'])

#Create a unique key column to import in postgresql
opportunities['ID'] = opportunities.reset_index().index

#Rename some columns
opportunities = opportunities.rename(columns={"display_name_x": "Opportunity Name", "stage_id.name": "Stage", "display_name_y": "Tags", "name": "Industry", "id - display_name": "ID Opportunity Name"})

print(opportunities[opportunities['Opportunity Name'].str.contains('DOW')])

count = 0
for c in opportunities.index:
  string_of_interest = ''
  if opportunities.loc[count, 'Tags']:
     string_of_interest = opportunities.loc[count,'Tags']
     print('For opportunity:', opportunities.loc[count, 'Opportunity Name'], ' count is: ', count, ' and String of Interest is: ', string_of_interest)
     if string_of_interest:
        if "Segment" in str(string_of_interest):
            opportunities.loc[count,'Segment'] = string_of_interest.replace("Segment: ", "")
        if "Product" in str(string_of_interest):
            opportunities.loc[count,'Product'] = string_of_interest.replace("Product: ", "")
        if "Type" in str(string_of_interest):
            opportunities.loc[count,'Type'] = string_of_interest.replace("Type: ", "")
  count = count+1

df = opportunities

print(opportunities[opportunities['Opportunity Name'].str.contains('DOW')])

opportunities = opportunities.drop(columns={'Tags', 'Product', 'Segment', 'Type'})
opportunities = opportunities.drop_duplicates(subset=['opportunity_UID'])

df = df.groupby(['opportunity_UID']).agg({'Product': lambda x: flatten_list(list(x)), 'Type': lambda x: flatten_list(list(x)),'Segment': lambda x: flatten_list(list(x))}).reset_index()

opportunities = df.merge(opportunities, left_on='opportunity_UID', right_on='opportunity_UID')

#Define the string to connect to VOCSens salesflare opportunities
url = "https://api.salesflare.com/opportunities"
headers = CaseInsensitiveDict()
headers["Authorization"] = "Bearer uBRh7NdEEyzMms-QI9VwToMF7EFOTMcAmYFcCwSBKXzQm"

#Request opportunities and load the dataframe
qualified_leads = requests.get(url, headers=headers)

qualified_leads = pd.read_json(qualified_leads.content)

df = pd.DataFrame(qualified_leads['custom'])

for c in df:
   if str(df[c].dtype) in ('object', 'string_', 'unicode_'):
        df[c].fillna(value=np.nan, inplace=True)

df.loc[df.custom.str.len() == 0, "custom"] = np.nan

df.custom = df.custom.replace(np.nan,'',regex=True)

count = 0
for c in df.custom:
  if c:
    c= c.get('segment')
    if c :
      qualified_leads.loc[count,'Segment'] = c['name']
  else:
    c = "N/A"
  count = count+1

#Find Salesflare inputs with stage being either Lead, Qualified Lead or Contacted Lead

df = pd.DataFrame(qualified_leads['stage'])

for c in df:
   if str(df[c].dtype) in ('object', 'string_', 'unicode_'):
        df[c].fillna(value=np.nan, inplace=True)

df.loc[df.stage.str.len() == 0, "stage"] = np.nan

df.stage = df.stage.replace(np.nan,'',regex=True)

count = 0
for c in df.stage:
  qualified_leads.loc[count,'new_stage_name'] = "N/A"
  if c:
    qualified_leads.loc[count,'new_stage_name'] = c['name']
  else:
    c = "N/A"
  #print(qualified_leads.loc[count, 'new_stage_name'])
  count = count+1

qualified_leads = qualified_leads[qualified_leads["new_stage_name"].str.contains("Lead")]
qualified_leads = qualified_leads.drop(columns=['stage'])
qualified_leads = qualified_leads.rename(columns={"new_stage_name": "stage_name"})

#Define the string to connect to VOCSens salesflare accounts
url = "https://api.salesflare.com/accounts"
headers = CaseInsensitiveDict()
headers["Authorization"] = "Bearer uBRh7NdEEyzMms-QI9VwToMF7EFOTMcAmYFcCwSBKXzQm"

payload = {"limit": 10000}

#Request accounts  and load the dataframe
salesflare_accounts = requests.get(url, headers=headers, params=payload)
salesflare_accounts = pd.read_json(salesflare_accounts.content)

#Look for the address jason column and drill down to the first address found
salesflare_accounts_addresses = pd.DataFrame(salesflare_accounts['addresses'].values.tolist())[0]

#Look for the country name in the address json field

count = 0
for c in salesflare_accounts_addresses:
  if c:
    c= c.get('country', "N/A")
    if c :
      #print('count is: ', count,' and value is: ', c)
      salesflare_accounts.loc[count, 'country'] = c
  else:
    c = "N/A"
  count = count+1


#Drop some unused columns
salesflare_accounts = salesflare_accounts.drop(columns=['tags', 'can_edit', 'custom', 'owner', 'domain', 'phone_numbers', 'email_addresses', 'picture', 'size', 'website', 'social_profiles', 'description', 'last_meeting_date', 'last_email_date', 'creation_date', 'modification_date', 'hotness', 'last_interaction_date', 'part_of', 'last_interaction', 'addresses'])

#Convert country names to country codes using pycountry
for row in salesflare_accounts.itertuples():
   country = salesflare_accounts.at[row.Index, 'country']
   if isinstance(country, str):
     salesflare_accounts.at[row.Index, 'country_id.code'] = coco.convert(country, to='ISO2')  # return the key's value if it exists

#salesflare_accounts = salesflare_accounts.drop(columns=['owner.id','owner.name','owner.email','owner.picture','name','domain','picture','size','website','description','last_interaction_date','creation_date','modification_date','part_of','hotness','can_edit','last_interaction.type','last_interaction.description','last_interaction.date','last_interaction.person.id','last_interaction.person.picture','last_interaction.person.name','last_email_date','last_meeting_date','social_profiles_0.id','social_profiles_0.type','social_profiles_0.url','social_profiles_0.username','social_profiles_1.id','social_profiles_1.type','social_profiles_1.url','social_profiles_1.username','social_profiles_2.id','social_profiles_2.type','social_profiles_2.url','social_profiles_2.username','social_profiles_3.id','social_profiles_3.type','social_profiles_3.url','social_profiles_3.username','social_profiles_4.id','social_profiles_4.type','social_profiles_4.url','social_profiles_4.username','addresses_0.id','addresses_0.city','addresses_0.country','addresses_0.region','addresses_0.state_region','addresses_0.street','addresses_0.type','addresses_0.zip','addresses_1.id','addresses_1.city','addresses_1.country','addresses_1.region','addresses_1.state_region','addresses_1.street','addresses_1.type','addresses_1.zip','addresses_2.id','addresses_2.city','addresses_2.country','addresses_2.region','addresses_2.state_region','addresses_2.street','addresses_2.type','addresses_2.zip','addresses_3.id','addresses_3.city','addresses_3.country','addresses_3.region','addresses_3.state_region','addresses_3.street','addresses_3.type','addresses_3.zip','addresses_4.id','addresses_4.city','addresses_4.country','addresses_4.region','addresses_4.state_region','addresses_4.street','addresses_4.type','addresses_4.zip','email_addresses_0.id','email_addresses_0.email','email_addresses_1.id','email_addresses_1.email','email_addresses_2.id','email_addresses_2.email','email_addresses_3.id','email_addresses_3.email','email_addresses_4.id','email_addresses_4.email','phone_numbers_0.id','phone_numbers_0.number','phone_numbers_0.type','phone_numbers_1.id','phone_numbers_1.number','phone_numbers_1.type','phone_numbers_2.id','phone_numbers_2.number','phone_numbers_2.type','phone_numbers_3.id','phone_numbers_3.number','phone_numbers_3.type','phone_numbers_4.id','phone_numbers_4.number','phone_numbers_4.type','tags_0.id','tags_0.name','tags_1.id','tags_1.name','tags_2.id','tags_2.name','tags_3.id','tags_3.name','tags_4.id','tags_4.name','custom.industry'])

qualified_leads['owner_name'] = pd.DataFrame(qualified_leads['owner'].values.tolist())['name']
qualified_leads['account_name'] = pd.DataFrame(qualified_leads['account'].values.tolist())['name']
qualified_leads['account_id'] = pd.DataFrame(qualified_leads['account'].values.tolist())['id']
qualified_leads['probability'] = 10
qualified_leads['expected_revenue'] = qualified_leads['calculated_value'].astype(float) / 10

#print(qualified_leads['account_id'])
#print(salesflare_accounts['id'])

qualified_leads = pd.merge(qualified_leads, salesflare_accounts, left_on='account_id', right_on='id')

qualified_leads['close_date'] = pd.to_datetime(qualified_leads['close_date'], 'coerce')

#Remove some unused columns
qualified_leads = qualified_leads.drop(columns = ['value', 'closed', 'creation_date', 'pipeline', 'last_interaction', 'last_stage_change_date', 'tags', 'custom', 'can_edit', 'start_date', 'account_id', 'id_y', 'assignee', 'frequency', 'units', 'contract_start_date', 'contract_end_date', 'recurring_price_per_unit', 'currency', 'done', 'creator', 'last_modified_by', 'modification_date', 'id_x', 'owner', 'account', 'lost_reason'])

#Rename some columns
qualified_leads = qualified_leads.rename(columns={"stage_name": "Stage", "close_date": "date_deadline", "account_name": "partner_name", "name": "Opportunity Name", "owner_name": "user_id.name", "id": "ID", "expected_revenue": "Net Revenue", "calculated_value": "expected_revenue"})

#print('Qualified Leads\n', qualified_leads)
#print('Opportunities', opportunities)

concat = pd.concat([opportunities, qualified_leads], ignore_index=True)

concat['priority'] = concat['priority'].astype(float)
concat['probability'] = concat['probability'].astype(float)

print('\n\nAnd the result of the concatenation is: \n',concat)

q2 = """select '1 - Strategic Accounts' as Source, partner_name as Account, Segment, Product, Type, sum(expected_revenue), date_deadline as closing_date from concat where priority = 3 group by date_deadline, partner_name order by partner_name ASC"""

q6 = """select '2 - Other Prospects' as Source, partner_name as Account, Segment, Product, Type, sum(expected_revenue), date_deadline as closing_date from concat where priority < 3 group by date_deadline, partner_name order by partner_name ASC"""

q7 = """select '3 - Qualified Leads' as Source, partner_name as Account, Segment, Product, Type, sum(expected_revenue), date_deadline as closing_date from concat where Stage like '%Lead' group by date_deadline, partner_name order by partner_name ASC """

#STRATEGIC_ACCOUNTS_table = sqldf(q1)
STRATEGIC_ACCOUNTS_DETAILS_table = sqldf(q2)
#PROSPECTS_table = sqldf(q3)
#LEADS_table = sqldf(q4)
#GRAND_TOTAL_table = sqldf(q5)
PROSPECTS_DETAILS_table = sqldf(q6)
LEADS_DETAILS_table = sqldf(q7)

#FINAL_table = pd.concat([STRATEGIC_ACCOUNTS_table, STRATEGIC_ACCOUNTS_DETAILS_table, PROSPECTS_table, PROSPECTS_DETAILS_table, LEADS_table, LEADS_DETAILS_table], ignore_index=True)
FINAL_table = pd.concat([STRATEGIC_ACCOUNTS_DETAILS_table, PROSPECTS_DETAILS_table, LEADS_DETAILS_table], ignore_index=True)

FINAL_table['closing_date'] = pd.to_datetime(FINAL_table['closing_date'], 'coerce')

print(FINAL_table)

#Create the connection to postgresql database on postgres.latitude45.biz server
engine = create_engine('postgresql+psycopg2://postgres:Evre57OyEiZwEbU!@localhost:55432/vocsens_sales_pipeline')

#Load the opportunities dataframe into postgres
concat.to_sql('vocsens_sales_pipeline', con=engine, if_exists= 'replace', index=False, method = 'multi')
FINAL_table.to_sql('vocsens_CRM_table', con=engine, if_exists= 'replace', index=False, method = 'multi')

