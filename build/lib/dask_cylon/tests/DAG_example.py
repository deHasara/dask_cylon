from itertools import product
import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask import delayed
import dask

df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                    'B': ['B0', 'B1', 'B2', 'B3'],
                    'C': ['C0', 'C1', 'C2', 'C3'],
                    'D': ['D0', 'D1', 'D2', 'D3']},
                   index=[0, 1, 2, 3])
df2 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                    'B1': ['B4', 'B5', 'B6', 'B7'],
                    'C1': ['C4', 'C5', 'C6', 'C7'],
                    'D1': ['D4', 'D5', 'D6', 'D7']},
                    index=[0, 5, 6, 7])


if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)
    client
    ddf1 = dd.from_pandas(df1, npartitions=3)
    ddf2 = dd.from_pandas(df2, npartitions=6)
    concat_df = ddf1.merge(ddf2, on=['A'])
    concated_df = concat_df.compute()
    print(concated_df)
    concat_df.visualize(filename='transp36.svg')

#from dask.distributed import LocalCluster, Client

#cluster = LocalCluster(processes=False)
#client = Client(cluster)
#print(client)

