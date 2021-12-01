from dask import dataframe as dd
import pandas as pd
import datetime
import dask_cylon as dc
#import pycylon

from pycylon import CylonContext
from pycylon import DataFrame
import dask

import numpy as np

ctx: CylonContext = CylonContext(config=None, distributed=False)

nrows = 100

#df = {'col-0': [1, 2, 3], 'col-1': [4, 5, 6]}
df = {'col-0': list(range(100)), 'col-1':['cat' for i in range(100)]}#list(np.random.randint(low = 3,high=899,size=1000))}
df = DataFrame(df)
#df.set_index(range(1000).to)
#df.set_index([i for i in range(1000)])

print('first dataframe')
df

#df1 = {'col-0': [1, 2, 3], 'col-3': [7, 8, 9]}
df1 = {'col-0': list(range(100)), 'col-3':list(np.random.randint(low = 3,high=1000,size=100))}
df1 = DataFrame(df1)
#df1.set_index([0,1,999])
#df1.set_index([i for i in range(1000)])
df1


if __name__ == '__main__':

    from dask.distributed import Client
    client = Client()
    client

    ddf = dc.from_pycylon(df, npartitions=4, sort=False)
    ddf2 = dc.from_pycylon(df1, npartitions=5, sort=False)

    merge_df = ddf.merge(ddf2, on=['col-0'])
    print('merge df:',merge_df)
    start_time = datetime.datetime.now()
    print(merge_df.compute())
    merge_df.compute()
    ddf.merge(ddf2, on=['col-0'])
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    print(execution_time)
    result = merge_df.compute()
    print(merge_df.compute())
    #merge_df.visualize(filename='test.svg')
    #print('result:',result, type(result))
    #print(type(merge_df))
