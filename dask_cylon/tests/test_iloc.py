from pycylon import DataFrame
from pycylon import CylonContext
ctx: CylonContext = CylonContext(config=None, distributed=False)

nrows = 10

df0 = {'col-0': list(range(10)), 'col-1':list(range(10))}
df = DataFrame(df0)
df.set_index([0,1,2,3,4,5,6,7,8,9])
df1 = df.iloc[0:5]
df2 = df.iloc[5:10]

print(df1)
print(df2)
dsk = {}
name="from-pycylon-"
for i in range(2):

    if i==0:
        cdf = DataFrame(df.iloc[0:5].to_arrow())
        cdf.set_index(range(0,5))
    else:
        cdf = DataFrame(df.iloc[5:10].to_arrow())
        cdf.set_index(range(5,10))
    print('sliced df:', cdf, cdf.index.values)
    dsk[(name, i)] = cdf
print(dsk)

'''
import pandas as pd

df3 = pd.DataFrame(df0)
df4 = df3.iloc[0:5]
df5 = df3.iloc[5:10]

print(df4)
print(df5)

print(df1.index.values, df2.index.values)
print(df4.index.values, df5.index.values)
'''