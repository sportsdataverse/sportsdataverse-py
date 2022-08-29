import sportsdataverse as sdv

# df = sdv.retrosheet_people()
# df = sdv.retrosheet_franchises()
# df = sdv.retrosheet_ballparks()
# df = sdv.retrosheet_ejections()
# df.to_csv('test.csv',index=False)
df = sdv.retrosheet_game_logs_team(2000,2020,'playoffs')

print(df)

