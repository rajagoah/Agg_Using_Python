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


#********************************************* EXTRAPOLATING THE ABOVE LOGIC ON THE ENTIRE DATASET ********************************************

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

print('***************************** PRINTING THE DATA FRAME FOR NJ *****************************')
print(data_joined.head())


print('***************************** WRITING TO CSV *****************************')
#using the reset_index() method to allow all the columns to be printed in to the csv
data_joined.reset_index().to_csv('/Users/aakarsh.rajagopalan/Personal documents/Datasets for tableau/Tableau project dataset/data_count_By_Party.csv',  index = False)
