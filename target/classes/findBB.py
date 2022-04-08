import pandas as pd

names = ['TaxiID', 'StartDate', 'StartLong', 'StartLat', 'StartState', 'EndDate', 'EndLong', 'EndLat', 'EndState']
df = pd.read_csv("../../../data/2010_03.segments", delimiter=",", names=names)
df = df[['StartLong', 'StartLat', 'EndLong', 'EndLat']]
print(df.tail())
