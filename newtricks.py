import pandas as pd
import datetime as dt

# reading in files, engine is python due to c engine not supporting regex separators
registrantProfiles = pd.read_table('./registrant_profiles.txt', sep='<\^\>', engine='python')
registrations = pd.read_table('./registrations.txt', sep='<\^\>', engine='python')
zipCode = pd.read_table('./zipcode_region_ranges.txt', sep='<\^\>', engine='python')
today = dt.datetime.now()
fourMonths = today - pd.DateOffset(months=4)

# registrantProfiles

# cleaning registrantProfiles by removing errant '"'
registrantProfiles['registrant_ID'] = registrantProfiles['registrant_ID'].map(lambda x: str(x).lstrip('"'))
registrantProfiles['zip_code'] = registrantProfiles['zip_code'].map(lambda x: str(x).lstrip('"').rstrip('"'))

# removing incorrect entry with nan for several values
registrantProfiles = registrantProfiles[registrantProfiles.loc[:,'registrant_ID'] != 'nan']

# formatting zip code correctly to be derived after merge (object to int)
registrantProfiles['zip_code'] = registrantProfiles['zip_code'].astype(int)

# converting Excel serial date to datetime
registrantProfiles['dob_formatted'] = pd.TimedeltaIndex(registrantProfiles['dob'], unit='d') + dt.datetime(1900, 1, 1)

# converting registrant_ID to int for merge as registrations['registrant_ID'] is int
registrantProfiles['registrant_ID'] = registrantProfiles['registrant_ID'].astype(int)

# dropping duplicate students in registrantProfiles as several dogs tied to same ID
registrantProfiles.drop_duplicates(subset=['dog_name', 'dob'], inplace=True)

registrantProfilesTotal = len(registrantProfiles)
print('Total number of Students: {} '.format((registrantProfilesTotal)))

# registrations

# Creating Date column in registrations
registrations['dor'] = pd.TimedeltaIndex(registrations['registration_date'], unit='d') + dt.datetime(1900, 1, 1)

# sort registrations by id and date of registration
registrations = registrations.sort_values(['registrant_ID', 'dor'])

#drop duplicates, keeping most recent date
registrations.drop_duplicates(subset=['registrant_ID'], keep='last', inplace=True)

# creating merged file, joining registrantProfiles and registrations on their respective registrant_ID column
merged = registrantProfiles.merge(registrations, left_on='registrant_ID', right_on='registrant_ID', how='inner')

# deriving region from given zip_code and ranges
zipcode_dict = zipCode.to_dict()
for i in range(0, len(zipCode)):
    merged.loc[(merged['zip_code'] >= zipcode_dict['min_zip_code'][i]) & (merged['zip_code'] <= \
        zipcode_dict['max_zip_code'][i]), 'zip_borough'] = zipCode['region_name'][i]

# filtering the list of students who have a primary coat of gold, and who registered in the last 4 months
gold = merged[(merged.loc[:, 'dominant_color'] == 'GOLD')]
gold = gold[(gold['dor'] >= fourMonths)]

# outputting total number of gold students and ID, .to_string to remove index for easy copying. Gold students also output as .csv
gold_total = len(gold)
print('Total number of Gold coats registered in past four months: {} '.format((gold_total)))
print('The students with a gold dominant color and date of registration within four months are:')
print(gold['registrant_ID'].to_string(index=False))

# Queens appeared twice in file, and helps if other regions are added in the future
regions = zipCode['region_name'].unique()

# for all rows in merged, if breed == 'Mixed/Other' place a 1 in the column mixed_Borough, where Borough is i in regions
for i in range(0, len(regions)):
    merged.loc[(merged['breed'] == 'Mixed/Other') & (merged['zip_borough'] == regions[i]), 'mixed_{}'.format(regions[i])] = 1

# create an ordered list of boroughs used
boroughs = merged['zip_borough'].value_counts().keys().tolist()
# create an ordered list of student count for each borough of same index above
counts = merged['zip_borough'].value_counts().tolist()

# output stats for each borough, including total student population, mixed breed, and percent of mixed breed to total 
boroughMixedPercent = {}
for i in range(0, len(boroughs)):
    mixedCount = int(merged['mixed_{}'.format(boroughs[i])].sum())
    percenMixed = round(mixedCount/counts[i]*100, 2)
    boroughMixedPercent[boroughs[i]] = percenMixed
    print("Region: {} has {} students, of which {} are 'Mixed/Other' breed ({}%)".format(boroughs[i], counts[i], mixedCount, percenMixed))

# create a list of lists containing the borough name and the percent 'Mixed/Other' breed, sorted by ascending percent
keyPairing = [(k, boroughMixedPercent[k]) for k in sorted(boroughMixedPercent, key=boroughMixedPercent.get, reverse=True)]

# take two lists containing borough name and smallest percentages
lowestTwo = keyPairing[-2:]

# output the two boroughs by lowest percentage of 'Mixed/Other' to overall population
print('The two districts with the smallest number of "Mixed/Other" students are {} with {}% and {} with {}%'.format(lowestTwo[1][0],\
   lowestTwo[1][1], lowestTwo[0][0], lowestTwo[0][1]))

# Outputting processed files
merged.to_csv('./newTricksProcessed.csv', sep=',', index=False)
gold.to_csv('./gold.csv', sep=',', index=False)