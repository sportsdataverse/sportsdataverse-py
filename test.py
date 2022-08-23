import sportsdataverse as sdv

df = sdv.retrosheet_people()
df = sdv.retrosheet_franchises()
df = sdv.retrosheet_ballparks()
df = sdv.retrosheet_ejections()
df.to_csv('test.csv',index=False)
print(df)

