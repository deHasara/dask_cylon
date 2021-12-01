import dask_cylon as dc


from pycylon import CylonContext
from pycylon import DataFrame

nrows = 10000000

df = {'a': [1, 2, 3], 'b': [4, 5, 6]}
df2 = DataFrame(df)
ddf2 = dc.from_pycylon(df2, npartitions=5, sort=False)
ddf2['c'] = ddf2['a'] + 5
ddf2