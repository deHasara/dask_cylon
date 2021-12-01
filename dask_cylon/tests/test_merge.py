from pycylon import CylonContext
from pycylon import DataFrame

import numpy as np

import pandas as pd

ctx: CylonContext = CylonContext(config=None, distributed=False)



df0 = {'col-0': list(range(100)), 'col-1': list(np.random.randint(low = 3,high=1000,size=100))}
df = DataFrame(df0)
df.set_index([i for i in range(100)])

pdf = pd.DataFrame(df0)

df1 = {'col-0': list(range(1000)), 'col-3':list(np.random.randint(low = 3,high=1000,size=1000))}
df2 = DataFrame(df1)
df2.set_index([i for i in range(1000)])

pdf1 = pd.DataFrame(df1)

kwargs ={'how': 'inner', 'left_on': ['col-0'], 'right_on': ['col-0'], 'left_index': False, 'right_index': False, 'suffixes': ('_x', '_y'), 'indicator': False}

merged = df.merge(df2, left_on= ['col-0'], right_on= ['col-0'])

merged_pdf = pdf.merge(pdf1, **kwargs)

print(merged)

print(merged_pdf)

#in pandas, if left_on col not equal right_on col names and if any other columns in two dfs, had same name suffixes are added to those columns
#But if left on == right on, no suffixes added

df1 = pd.DataFrame({'rkey': ['foooo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]})
df2 = pd.DataFrame({'rkey': ['foo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]})