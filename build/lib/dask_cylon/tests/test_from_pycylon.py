from dask import dataframe as dd
import pandas as pd

import dask_cylon as dc
#import pycylon

from pycylon import CylonContext
from pycylon import DataFrame
import dask

ctx: CylonContext = CylonContext(config=None, distributed=False)

nrows = 10000000

#df = {'col-0': [1, 2, 3], 'col-1': [4, 5, 6]}
df = {'col-0': list(range(1000)), 'col-1':list(range(1000))}
df = DataFrame(df)
df.set_index([0,1,999])
print('first dataframe')
df

#df1 = {'col-0': [1, 2, 3], 'col-3': [7, 8, 9]}
df1 = {'col-0': list(range(1000)), 'col-3':list(range(1000))}
df1 = DataFrame(df1)
df1.set_index([0,1,999])
df1


if __name__ == '__main__':

    from dask.distributed import Client
    client = Client()
    client

    ddf = dc.from_pycylon(df, npartitions=2, sort=False)
    ddf2 = dc.from_pycylon(df1, npartitions=2, sort=False)

    merge_df = ddf.merge(ddf2, on=['col-0'])
    print('merge df:',merge_df)
    result = merge_df.compute()
    #print(merge_df.compute())
    print('result:',result, type(result))
