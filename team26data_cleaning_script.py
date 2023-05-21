#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals()) 


# In[2]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\Travis_County_4-Year_High_School_Graduation_Rates_by_Campus.csv"

df = pd.read_csv (path)
list(df.columns)


# In[3]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\District-Type.csv"
district_type = pd.read_csv (path)
list(district_type.columns)


# In[4]:


query = '''
SELECT *
FROM df
LEFT JOIN district_type
ON df.District_Number = district_type.District_Number'''

df = mysql(query)


# In[5]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\operatingexpenditures.csv"
op_expenditures = pd.read_csv (path)

op_expenditures.drop('District', axis=1, inplace=True)

list(op_expenditures.columns)


# In[6]:


year = 2016
df1_year = df[df.Year == year]
df2_year = op_expenditures[op_expenditures.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.District_Number = df2_year.District_Number'''

df2016 = mysql(query)


# In[7]:


year = 2017
df1_year = df[df.Year == year]
df2_year = op_expenditures[op_expenditures.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.District_Number = df2_year.District_Number'''

df2017 = mysql(query)


# In[8]:


year = 2018
df1_year = df[df.Year == year]
df2_year = op_expenditures[op_expenditures.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.District_Number = df2_year.District_Number'''

df2018 = mysql(query)


# In[9]:


year = 2019
df1_year = df[df.Year == year]
df2_year = op_expenditures[op_expenditures.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.District_Number = df2_year.District_Number'''

df2019 = mysql(query)


# In[10]:


year = 2020
df1_year = df[df.Year == year]
df2_year = op_expenditures[op_expenditures.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.District_Number = df2_year.District_Number'''

df2020 = mysql(query)


# In[11]:


current_step = pd.concat([df2016, df2017, df2018, df2019, df2020])


# In[12]:


current_step.shape


# In[13]:


a = current_step.iloc[:,26]
b = current_step.iloc[:,23]
list(current_step.columns)


# In[14]:


current_step.drop(current_step.columns[[23, 26]], axis=1, inplace=True)
list(current_step.columns)


# In[15]:


current_step['year'] = a
current_step['district_num'] = b


# In[16]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\traviscounty9-12TeacherSalaries.csv"
teachers = pd.read_csv (path)
teachers.drop(['County', 'District Name'], axis=1, inplace=True)
list(teachers.columns)


# In[17]:


year = 2016
df1_year = current_step[current_step.year == year]
df2_year = teachers[teachers.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.district_num = df2_year.District_Number'''

df2016 = mysql(query)
df2016.drop(['year', 'district_num'], axis=1, inplace=True)
list(df2016.columns)


# In[18]:


year = 2017
df1_year = current_step[current_step.year == year]
df2_year = teachers[teachers.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.district_num = df2_year.District_Number'''

df2017 = mysql(query)
df2017.drop(['year', 'district_num'], axis=1, inplace=True)


# In[19]:


year = 2018
df1_year = current_step[current_step.year == year]
df2_year = teachers[teachers.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.district_num = df2_year.District_Number'''

df2018 = mysql(query)
df2018.drop(['year', 'district_num'], axis=1, inplace=True)


# In[20]:


year = 2019
df1_year = current_step[current_step.year == year]
df2_year = teachers[teachers.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.district_num = df2_year.District_Number'''

df2019 = mysql(query)
df2019.drop(['year', 'district_num'], axis=1, inplace=True)


# In[21]:


year = 2020
df1_year = current_step[current_step.year == year]
df2_year = teachers[teachers.Year == year]

query = '''
SELECT *
FROM df1_year
LEFT JOIN df2_year
ON df1_year.district_num = df2_year.District_Number'''

df2020 = mysql(query)
df2020.drop(['year', 'district_num'], axis=1, inplace=True)


# In[22]:


next_step = pd.concat([df2016, df2017, df2018, df2019, df2020])


# In[23]:


next_step.drop(['Campus Number', 'City', 'Type of School', 'District Name', 'Travis County all students graduation rate', 'District_Number'], axis=1, inplace=True)
next_step.rename(columns = {'Zip Code':'zipcode'}, inplace=True)
list(next_step.columns)


# In[24]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\rural_urban.txt"
rural = pd.read_csv(path, sep="\t")
list(rural.columns)


# In[25]:


query = '''
SELECT *
FROM next_step
LEFT JOIN rural
ON next_step.zipcode = rural.ZCTA5'''

df = mysql(query)
df.drop('ZCTA5', axis=1, inplace=True)
list(df.columns)


# In[26]:


#path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\housing.txt"
#original = pd.read_csv(path, sep="\t")
path2 = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\revised_housing.csv"
housing = pd.read_csv(path2)
list(housing.columns)


# In[27]:


query = '''
SELECT *
FROM df
LEFT JOIN housing
ON df.zipcode = housing.ZCTA5'''

df = mysql(query)
df.drop('ZCTA5', axis=1, inplace=True)
list(df.columns)


# In[28]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\econ_revised.csv"
econ = pd.read_csv(path)
list(econ.columns)


# In[29]:


query = '''
SELECT *
FROM df
LEFT JOIN econ
ON df.zipcode = econ.ZCTA5'''

df = mysql(query)
df.drop('ZCTA5', axis=1, inplace=True)
list(df.columns)


# In[30]:


path = "C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\demographics.csv"
demo = pd.read_csv(path)
list(demo.columns)


# In[31]:


query = '''
SELECT *
FROM df
LEFT JOIN demo
ON df.zipcode = demo.ZCTA5'''

df = mysql(query)
df.drop('ZCTA5', axis=1, inplace=True)
list(df.columns)


# In[32]:


df[['TotalOperatingExpenditures', 'OperatingExpendituresPerStudent']] = df[['TotalOperatingExpenditures', 'OperatingExpendituresPerStudent']].replace({'\$':''}, regex = True)
df[['TotalOperatingExpenditures', 'OperatingExpendituresPerStudent']] = df[['TotalOperatingExpenditures', 'OperatingExpendituresPerStudent']].replace({'\,':''}, regex = True)


# In[33]:


columns = ['Campus Name', 'zipcode', 'District', 'TEA_District_Type', 'NCES_District_Type', 'NCES_Description', 'Charter School',
          'urban_rural-population-urban_total_population', 'urban_rural-population-rural_total_population', 'Total_Units_of_Occupied_Housing',
          'Total_Vacant_Units', 'Percentage_of_Homes_for_Sale', 'Number_of_Mobile_Homes', 'percent-employed-of-population_16_years_and_over',
          'employment_status-percent-not_in_labor_force-of-population_16_years_and_over',
          'income_and_benefits_in_2016_inflation_adjusted_dollars-households-mean_retirement_income_dollars_households_with_retirement_income',
          'Percentage_of_Vacant_Units', 'Number_of_Homes_Occupied_by_Owner', 'Total_Homes_Occupied_by_Renter',
           'income_and_benefits_in_2016_inflation_adjusted_dollars-dollars-mean_earnings_dollars_households_with_earnings',
          'Percent_of_pop_18_years_and_over',]
df.drop(columns, axis=1, inplace=True)


# In[34]:


list(df.columns)


# In[35]:


rename_dict = {'Campus all students graduation rate':'GRAD_RATE_OVERALL',
 'Campus African American graduation rate': 'GRAD_RATE_BLACK',
 'Campus Asian graduation rate':'GRAD_RATE_ASIAN',
 'Campus Hispanic graduation rate':'GRAD_RATE_HISPANIC',
 'Campus White graduation rate':'GRAD_RATE_WHITE',
 'Campus economically disadvantaged graduation rate':'GRAD_RATE_ECONOMIC_DISADVANTAGE',
 'Campus female graduation rate':'GRAD_RATE_FEMALE',
 'Campus male graduation rate':'GRAD_RATE_MALE',
 'TEA_Description':'DISTRICT_TYPE',
 'TotalOperatingExpenditures':'TOTAL_OP_EXPENDITURE',
 'OperatingExpendituresPerStudent':'OP_EXPENDITURE_PER_STUDENT',
 'FTE Count':'FTE_COUNT',
 'Total Base Pay':'TOTAL_SALARY_SPEND',
 'Average Base Pay':'AVG_TEACHER_SALARY',
 'Year': 'YEAR',
 'urban_rural-population-total_population':'TOTAL_POP',
 'urban_rural-percent-urban_population-of-total_population':'PERCENT_URBAN',
 'urban_rural-percent-rural_population-of-total_population':'PERCENT_RURAL',
 'Total_Units_of_Housing':'TOTAL_HOUSING_AVAILABLE',
 'Percent_of_Housing_Occupied':'PERCENT_HOUSING_OCCUPIED',
 'Mobile_Homes_as_Percentage_of_Total_Housing':'MOBILE_HOMES_PERCENTAGE_OF_HOUSING',
 'Percentage_of_Homes_Occupied_by_Owner':'PERCENTAGE_OF_HOMES_OWNER_OCCUPIED',
 'Percentage_of_Homes_Occupied_by_Renter': 'PERCENTAGE_OF_HOMES_RENTED',
 'Average_Household_Size_Owner_Occupied_Unit':'AVERAGE_HOUSEHOLD_SIZE_OWNED',
 'Average_Household_Size_Rented_Unit':'AVERAGE_HOUSEHOLD_SIZE_RENTED',
 'Percentage_of_Occupied_Homes_with_no_vehicle':'PERCENT_OF_HOMES_W_NO_VEHICLE',
 'Percent_of_homes_valued_less_than_50000_owner_occupied':'PERCENT_OF_HOMES_VALUED_LESS_THAN_50000',
 'Percent_of_homes_valued_50000_to_99999_owner_occupied':'PERCENT_OF_HOMES_VALUED_50000_to_99999',
 'Percent_of_homes_valued_100000_to_149999_owner_occupied':'PERCENT_OF_HOMES_VALUED_100000_TO_149999',
 'Percent_of_homes_valued_150000_to_199999_owner_occupied':'PERCENT_OF_HOMES_VALUED_150000_TO_199999',
 'Percent_of_homes_valued_200000_to_299999_owner_occupied':'PERCENT_OF_HOMES_VALUED_200000_TO_299999',
 'Percent_of_homes_valued_300000_to_499999_owner_occupied':'PERCENT_OF_HOMES_VALUED_300000_TO_499999',
 'Percent_of_homes_valued_500000_to_999999_owner_occupied':'PERCENT_OF_HOMES_VALUED_500000_TO_999999',
 'Percent_of_homes_valued_1000000_or_more_owner_occupied':'PERCENT_OF_HOMES_VALUED_1000000_OR_MORE',
 'median_home_value':'MEDIAN_HOME_VALUE',
 'Percent_of_homes_with_mortgage':'PERCENTAGE_OF_HOMES_W_MORTGAGE',
 'Percent_of_homes_no_mortgage':'PERCENTAGE_OF_HOMES_W_NO_MORTGAGE',
 'Percentage_of_renters_paying_less_than_500':'PERCENTAGE_OF_RENTERS_PAYING_LESS_THAN_500',
 'Percentage_of_Renters_paying_500_to_999':'PERCENTAGE_OF_RENTERS_PAYING_500_TO_999',
 'Percentage_of_Renters_paying_1000_to_1499':'PERCENTAGE_OF_RENTERS_PAYING_1000_TO_1499',
 'Percentage_of_Renters_paying_1500_to_1999':'PERCENTAGE_OF_RENTERS_PAYING_1500_TO_1999',
 'Percentage_of_Renters_paying_2000_to_2499':'PERCENTAGE_OF_RENTERS_PAYING_2000_TO_2499',
 'Percentage_of_renters_paying_2500_to_2999':'PERCENTAGE_OF_RENTERS_PAYING_2500_TO_2999',
 'Percentage_of_Renters_paying_3000_or_more':'PERCENTAGE_OF_RENTERS_PAYING_3000_OR_MORE',
 'Median_Rent':'MEDIAN_RENT',
 'Rent_as_a_percentage_of_household_income':'RENT_AS_PERCENT_OF_INCOME',
 'population_16_years_and_over':'POP_16_YEAR_AND_OVER',
 'percent_in_labor_force_of_population_16_years_and_over':'PERCENT_OF_LABOR_16_YEAR_AND_OVER',
 'percent-unemployed-of-population_16_years_and_over':'PERCENT_UNEMPLOYED_16_YEAR_AND_OVER',
 'percent-in_labor_force-of-females_16_years_and_over':'PERCENT_OF_LABOR_FEMALE_AND_16_AND_OVER',
 'percent-employed-of-females_16_years_and_over':'PERCENT_EMPLOYED_FEMALE_AND_16_AND_OVER',
 'percent_of_households_with_children_under_6_where_all_parents_work':'PERCENT_OF_HOMES_WITH_CHILDREN_UNDER_6_BOTH_PARENTS_WORK',
 'Number_of_homes_with_children_aged_6_to_17_years':'NUM_OF_HOMES_WITH_CHILDREN_6_TO_17_YEARS',
 'Percent_of_homes_with_children_6_to_17_where_all_parents_work':'PERCENT_OF_HOMES_WITH_CHILDREN_6_TO_17_BOTH_PARENTS_WORK',
 'percent_of_households_with_income_and_benefits_in_2016_inflation_adjusted_dollars-percent-less_than_10000':'PERCENT_W_INCOME_LESS_THAN_10000',
 'Percent_of_homes_with_income_and_benefits_in_2016_inflation_adjusted_dollars-10000_to_14999':'PERCENT_W_INCOME_10000_TO_14999',
 'Percent_of_homes_with_income_and_benefits_in_2016_inflation_adjusted_dollars_15000_to_24999':'PERCENT_W_INCOME_15000_TO_24999',
 'percent_of_homes_with_income_and_benefits_in_2016_inflation_adjusted_dollars-25000_to_34999':'PERCENT_W_INCOME_25000_To_34999',
 'Percent_of_homes_with_income_and_benefits_in_2016_inflation_adjusted_dollars-35000_to_49999':'PERCENT_W_INCOME_35000_TO_49999',
 'Percent_of_homes_with_income_and_benefits_in_2016_inflation_adjusted_dollars-50000_to_74999':'PERCENT_W_INCOME_50000_TO_74999',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-75000_to_99999-of-total_households':'PERCENT_W_INCOME_75000_TO_99999',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-100000_to_149999-of-total_households':'PERCENT_W_INCOME_100000_TO_149999',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-150000_to_199999-of-total_households':'PERCENT_W_INCOME_150000_TO_199999',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-200000_or_more-of-total_households':'PERCENT_W_INCOME_200000_OR_MORE',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-dollars-median_household_income_dollars':'MEDIAN_HOUSEHOLD_INCOME',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-dollars-mean_household_income_dollars':'MEAN_HOUSEHOLD_INCOME',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-households_with_social_security-of-total_households':'PERCENT_ON_SOCIAL_SECURITY',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-households_with_retirement_income-of-total_households':'PERCENT_HOUSEHOLDS_RETIRED',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-with_cash_public_assistance_income-of-total_households':'PERCENT_RECIEVING_PUBLIC_ASSISTANCE',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-dollars-mean_cash_public_assistance_income_dollars_households_with_cash_public_assistance_income':'MEAN_INCOME_IF_PUBLIC_ASSISTANCE',
 'income_and_benefits_in_2016_inflation_adjusted_dollars-percent-households_with_food_stampsnap_benefits_in_the_past_12_months-of-total_households':'PERCENT_RECEIVING_FOOD_STAMPS',
 'Percentage_without_health_insurance':'PERCENT_NO_HEALTH_INSURANCE',
 'Percentage_of_children_without_health_insurance':'PERCENT_CHILDREN_NO_HEALTH_INSURANCE',
 'Percentage_of_families_with_children_below_poverty_level':'PERCENT_FAMILIES_W_CHILDREN_BELOW_POVERTY',
 'Percentage_of_Pop_Male':'PERCENT_POP_MALE',
 'Percentage_of_Pop_Female':'PERCENT_POP_FEMALE',
 'Percent_of_Pop_Under_5':'PERCENT_POP_UNDER_5',
 'Percentage_of_Pop_5_to_9':'PERCENT_POP_5_TO_9',
 'Percentage_of_Pop_10_to_14':'PERCENT_POP_10_TO_14',
 'Percentage_of_Pop_15_to_19_years':'PERCENT_POP_15_TO_19',
 'Percentage_of_Pop_20_to_24_years':'PERCENT_POP_20_TO_24',
 'Percentage_of_Pop_25_to_34_years':'PERCENT_POP_25_34',
 'Percentage_of_Pop_35_to_44_years':'PERCENT_POP_35_TO_44',
 'Percentage_of_Pop_45_to_54':'PERCENT_POP_45_TO_54',
 'Percentage_of_Pop_55_to_59_years':'PERCENT_POP_55_TO_59',
 'Percentage_of_Pop_60_to_64_years':'PERCENT_POP_60_TO_64',
 'Percentage_of_Pop_65_to_74_years':'PERCENT_POP_65_TO_74',
 'Percentage_of_pop_75_to_84_years':'PERCENT_POP_75_TO_84',
 'Percentage_of_pop_85_years_and_over':'PERCENT_POP_85_OR_OLDER',
 'Median Population Age':'MEDIAN_POP_AGE',
 'Percentage of Pop White':'PERCENT_POP_WHITE',
 'Percentage of Pop Black':'PERCENT_POP_BLACK',
 'Percent_of_pop_american_indian_and_alaska_native':'PERCENT_POP_AMINDIAN_NATIVE',
 'Percent_of_pop_asian':'PERCENT_POP_ASIAN',
 'Percent_of_pop_native_hawaiian_and_other_pacific_islander':'PERCENT_POP_HAWAII_PAC_ISL',
 'Percent_of_pop_other':'PERCENT_POP_OTHER',
 'Percent_identifying_as_hispanic_or_latino':'PERCENT_POP_HISPANIC'
}


# In[36]:


df.rename(columns=rename_dict, inplace=True)
list(df.columns)


# In[37]:


df.GRAD_RATE_BLACK = df.GRAD_RATE_BLACK.fillna(df.GRAD_RATE_OVERALL)
df.GRAD_RATE_ASIAN = df.GRAD_RATE_ASIAN.fillna(df.GRAD_RATE_OVERALL)
df.GRAD_RATE_WHITE = df.GRAD_RATE_WHITE.fillna(df.GRAD_RATE_OVERALL)
df.GRAD_RATE_FEMALE = df.GRAD_RATE_FEMALE.fillna(df.GRAD_RATE_OVERALL)
df.GRAD_RATE_MALE = df.GRAD_RATE_MALE.fillna(df.GRAD_RATE_OVERALL)
df.dropna(subset=['TOTAL_OP_EXPENDITURE', 'OP_EXPENDITURE_PER_STUDENT', 'FTE_COUNT', 'TOTAL_SALARY_SPEND',
 'AVG_TEACHER_SALARY', 'YEAR',], inplace=True)
df['MEAN_INCOME_IF_PUBLIC_ASSISTANCE'].fillna(value=df['MEAN_INCOME_IF_PUBLIC_ASSISTANCE'].mean(), inplace=True)


# In[38]:


df = pd.get_dummies(df, columns=['DISTRICT_TYPE'])
df = df[df.GRAD_RATE_OVERALL > 0]


# In[39]:


df.to_csv("C:\\Users\\sudip\\OneDrive\\Desktop\\MGMT Project Data\\master_datafile.csv")


# In[41]:


df.shape

