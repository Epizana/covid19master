import pandas as pd
#import numpy as np
import wget
import tabula
from datetime import date
import datetime
from pathlib import Path

today = date.today()

folderpath = Path('/Users/Emanuel/Documents/2019 Plan/NYC Data Science Academy/Post Bootcamp/COVID-19/Forecasting/covid19-global-forecasting-week-1/nyc')

# Read raw data from nyc.gov and convert PDFs into CSV, read CSVs into DataFrames.

urls = ['https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary-hospitalizations.pdf',
        "https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary-deaths.pdf",
        'https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary.pdf']

names = ['/Users/Emanuel/Documents/2019 Plan/NYC Data Science Academy/Post Bootcamp/COVID-19/Forecasting/covid19-global-forecasting-week-1/nyc/NYCH.csv',
         '/Users/Emanuel/Documents/2019 Plan/NYC Data Science Academy/Post Bootcamp/COVID-19/Forecasting/covid19-global-forecasting-week-1/nyc/NYCD.csv',
         '/Users/Emanuel/Documents/2019 Plan/NYC Data Science Academy/Post Bootcamp/COVID-19/Forecasting/covid19-global-forecasting-week-1/nyc/NYCC.csv']

for i in range(len(urls)):
    
    tabula.convert_into(urls[i],names[i], output_format="csv", pages='all')
    
nycd = pd.read_csv(folderpath / 'NYCD.csv', header = None)
nych = pd.read_csv(folderpath / 'NYCH.csv', header = None)
nycc = pd.read_csv(folderpath / 'NYCC.csv')

#helper functions to read in data and split into several other dataframes:

def getIndices(df, value):
    '''Given a dataframe and a value, returns the index of the value
       within the dataframe, otherwise returns -1.
    '''
    #get bool dataframe showing whether the object is actually in the dataframe or not.
    booldf = df.isin([value])
    #show which columns have the object with bool True
    boolseries = booldf.any()
    #shrink the result so that only True shows up. Store the successful columns in a list.
    colnames = list(boolseries[boolseries == True].index)
    #iterate through the columns to check
    
    results = []
    
    for col in colnames:
        #store all matching rows into a list
        rows = list(booldf[col][booldf[col] == True].index)
        for row in rows:
            results.append((row,col)) #append tuple of row, column to result list.
    
    return results
    

#format the value column.
def countform(rowitem):
    try:
        return int(rowitem[:rowitem.find('(')-1])
    except:
        return int(rowitem)

#PDFs contain several categories. Identify and split them out based on deaths, hospitalizations, and confirmed cases.
dcats = ['Age Group','Sex','Borough','Total']
hcats = ['Age Group','Sex','Borough','Total']
ccats = ['Age Group','Age 50 and over','Sex','Borough','Deaths']

#helper function to slice the original dataframe from the pdf and get new dataframes.

def getdfs(df,categories):
    
    for i in range(len(categories)-1):
    
        if df.equals(nych):    
            temp = df[getIndices(df,categories[i])[0][0]:getIndices(df,categories[i+1])[0][0]].rename({0:categories[i], 1:'Ever Hospitalized Cases', 2:'Total Cases'}, axis = 1).reset_index(drop=True)
        elif df.equals(nycd):
            temp = df[getIndices(df,categories[i])[0][0]:getIndices(df,categories[i+1])[0][0]].rename({0:categories[i],1:'Underlying Conditions',
                                                                                                       2:'No Underlying Conditions',3:'Underlying Conditions Pending',
                                                                                                       4:'Total Deaths'}, axis = 1).reset_index(drop=True)
            #temp.drop(['Unnamed: 1'], axis = 1, inplace = True)
        elif df.equals(nycc):
            temp = df[getIndices(df,categories[i])[0][0]:getIndices(df,categories[i+1])[0][0]].rename({'.':categories[i]}, axis = 1).reset_index(drop=True)
            temp.drop(['Unnamed: 1'], axis = 1, inplace = True)
        
        
        temp.drop(0, axis = 0, inplace = True)

        temp[categories[i]] = temp[categories[i]].map(lambda x: x[1:].lstrip())
        temp.iloc[:,1] = temp.iloc[:,1].apply(countform)
        temp['Date'] = today #today

        yield temp

#READ IN TODAY'S DATA INTO THE SPECIFIED DATAFRAMES


#CREATE DATAFRAMES
age_death,sex_death,borough_death = getdfs(nycd,dcats)
age_hosp,sex_hosp,borough_hosp = getdfs(nych,hcats)
age_cases,cases50new,sex_cases,borough_cases = getdfs(nycc,ccats)

#combines  several of the new dataframes into one
def mergedfs(leftdf,rightdf,on):
    temp = pd.merge(leftdf,rightdf, how='left', on = on)
    temp.rename({'Date_x':'Date'},axis = 1, inplace = True)
    temp.drop(['Date_y'], axis=1, inplace = True)
    temp = temp.fillna(0)
    return temp

#combine dataframes
age_combined = mergedfs(age_hosp,age_death,'Age Group')
sex_combined = mergedfs(sex_hosp,sex_death,'Sex')
borough_combined = mergedfs(borough_hosp,borough_death,'Borough')

#PREPARE TO CONCATENATE PRIOR DAY'S TOTALS BY READING IN PREVIOUS CSVs

def assemble_df_new(oldcsv,newdf): #newcols
    #for cols in newcols:
     #   olddf[cols] = 0
    olddf = pd.read_csv(folderpath / oldcsv)
    temp = pd.concat([olddf,newdf], axis = 0, sort = False).reset_index(drop = True)
    #temp = temp.fillna(0)
    return temp

#new totals by reading in prior totals and updating for today
agenew = assemble_df_new('age.csv',age_combined)
boroughnew = assemble_df_new('borough.csv',borough_combined)
sexnew = assemble_df_new('sex.csv',sex_combined)
cases50current = assemble_df_new('cases50.csv',cases50new)

#output updated CSVs

agenew.to_csv(folderpath / 'age.csv', index = False)
sexnew.to_csv(folderpath / 'sex.csv', index = False)
boroughnew.to_csv(folderpath / 'borough.csv', index = False)
cases50current.to_csv(folderpath / 'cases50.csv', index = False)


