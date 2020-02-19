#importing required modules
import pandas as pd

#configuring the display limits of the columns and rows
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows',15)

print('***************************** CONFIG COMPLETE *****************************')

#using read_csv file to create a data frame out of the input file
data = pd.read_csv('/Users/aakarsh.rajagopalan/Personal documents/Datasets for tableau/Tableau project dataset/countypres_2000-2016.csv')

#displaying the data type of the read data
print(type(data))

#displaying the shape of the datafram
print(data.shape)

print('***************************** DATA READ COMPLETE *****************************')

#sampling the data to the console
print(data.head())

#***********************************************************************************************************************************************************
#********************************************* LOGIC EXTRACT A CSV HAVING STATE, COUNTY, YEAR, VOTES_BY_PARTY, PARTY, CANDIDATE & VOTES********************************************
#***********************************************************************************************************************************************************

print('***************************** STATE, COUNTY, YEAR, VOTES_BY_PARTY, PARTY, CANDIDATE & VOTES *****************************')

#creating a dataframe with with candidatevotes aggregated across countys in states
data_cnty_sum = pd.DataFrame(data.groupby(['state','county','year','party','candidate']).agg(
        votes_by_party_cnty = pd.NamedAgg(column = 'candidatevotes', aggfunc = sum)
)).reset_index()

print('***************************** GENERATING UNIQUE STRING FOR DATA_CNTY_SUM *****************************')
#creating a new column called unique string. This unique string will be used to join with another dataframe
data_cnty_sum['Unique_string'] = data_cnty_sum['year'].map(str).str.strip()+\
                                 data_cnty_sum['state'].map(str).str.strip()+\
                                 data_cnty_sum['county'].map(str).str.strip()+\
                                 data_cnty_sum['votes_by_party_cnty'].map(str).str.strip()

print('***************************** GENERATING UNIQUE STRING FOR DATA_CNTY_SUM *****************************')
print(data_cnty_sum.head(1))
#creating dataframe with candidatevotes aggregated across county to find out who the winner was in a given year
data_cnty_winner = pd.DataFrame(data.groupby(['year','state','county']).agg(
        votes_by_party_cnty = pd.NamedAgg(column = 'candidatevotes', aggfunc = 'max')
)).reset_index()

print('***************************** GENERATING UNIQUE STRING FOR DATA_CNTY_WINNER *****************************')
#creating a new column called unique string. This unique string will be used to join with another dataframe
data_cnty_winner['Unique_string'] = data_cnty_winner['year'].map(str).str.strip()+\
                                 data_cnty_winner['state'].map(str).str.strip()+\
                                 data_cnty_winner['county'].map(str).str.strip()+\
                                 data_cnty_winner['votes_by_party_cnty'].map(str).str.strip()

print('***************************** PRITING SAMPLE RECORDS IN DATA_CNTY_WINNER *****************************')
print(data_cnty_winner.head(1))

#joining the data_cnty_sum and data_cnty_winner dataframes to output a new dataframe containing the party names of the winner
data_cnty_joined = pd.merge(data_cnty_winner, data_cnty_sum[['party','candidate','Unique_string']], on = 'Unique_string', how = 'left')

print('***************************** PRITING SAMPLE RECORDS IN DATA_CNTY_JOINED *****************************')
print(data_cnty_joined.head(1))

#priting out the dataframe to a csv
data_cnty_joined.reset_index().to_csv('/Users/aakarsh.rajagopalan/Personal documents/Datasets for tableau/Tableau project dataset/Data_count_county_level.csv',  index = False)


#***********************************************************************************************************************************************************
#********************************************* LOGIC TO EXTRACT A CSV HAVING STATE, YEAR, VOTES_BY_PARTY, PARTY, CANDIDATE & VOTES********************************************
#***********************************************************************************************************************************************************
print('***************************** EXECUTING THE CODE BLOCK THAT CALC THE VOTES BY STATE *****************************')
#grouping by state, year, party and candidate to extract count of candidate votes per state. this will sum the votes across all countys
data_sum = pd.DataFrame(data.groupby(['state','year','party','candidate']).agg(
        votes_by_party = pd.NamedAgg(column = 'candidatevotes', aggfunc = sum)
)).reset_index()

#adding a new column called UNIQUE_KEY. This column will be used to join with another dataframe
data_sum['Unique_Key'] = data_sum['state'].map(str).str.strip()+\
                            data_sum['year'].map(str).str.strip()+\
                            data_sum['votes_by_party'].map(str).str.strip()
print(data_sum.head())

#Building a new dataframe to group by state and year and aggregating to show max votes
print('***************************** FINDING OUT WHO WON IN NJ *****************************')
data_winner = data_sum.groupby(['state','year']).agg(
        {'votes_by_party':'max'}
).reset_index()

#printing out the shape of the dataframe
print(data_winner.shape)

#adding new column called UNIQUE_KEY. This column will be used to join with another dataframe
data_winner['Unique_Key'] = data_winner['state'].map(str).str.strip()+\
                               data_winner['year'].map(str).str.strip()+\
                               data_winner['votes_by_party'].map(str).str.strip()

#joining the "data_nj_sum" and the "data_nj_winner" dataframes to be able to assign the party name to the winner of the year's election
data_joined = pd.merge(data_winner, data_sum[['party','candidate','Unique_Key']], on = 'Unique_Key', how = 'left')
print(data_joined.head(1))

print('***************************** PRINTING THE DATA FRAME FOR STATE LEVEL *****************************')
print(data_joined.head())


print('***************************** WRITING TO CSV *****************************')
#using the reset_index() method to allow all the columns to be printed in to the csv
data_joined.reset_index().to_csv('/Users/aakarsh.rajagopalan/Personal documents/Datasets for tableau/Tableau project dataset/Data_count_state_level.csv',  index = False)
