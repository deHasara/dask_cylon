import pandas as pd
df = pd.DataFrame({'animal':['snake', 'bat', 'tiger', 'lion',
                   'fox', 'eagle', 'shark', 'dog', 'deer'], 'animal_copy':['snake', 'bat', 'tiger', 'lion',
                   'fox', 'eagle', 'shark', 'dog', 'deer']})
df1 = df.head(0)
#print(df1.iloc[:,0])
#print(df1.iloc[:,1])

for i, c in enumerate(df1.columns):
    series = df1.iloc[:, i]
    print(series)
    dt = series.dtype
    print(dt)