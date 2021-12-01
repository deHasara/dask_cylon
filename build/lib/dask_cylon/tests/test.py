#from dask_cylon.core import from_cudf
from math import ceil

from dask.core import flatten

import dask_cylon as dc

import pyarrow as pa
import numpy as np
import pandas as pd
import pycylon
from pycylon import Table
from pycylon import CylonContext
from pycylon import DataFrame
import dask


import time

ctx: CylonContext = CylonContext(config=None, distributed=False)

d3 = {'col-0': [1, 2, 3], 'col-1': [4, 5, 6]}
d4 = pd.DataFrame(d3)
cdf3 = DataFrame(d3)
cdf4 = DataFrame(d4)
ct = Table.from_pandas(ctx, d4)


#print(type(ct))
#print(type(cdf3))
#print(type(cdf4))

#ct.show()
#print(cdf3)
#print(cdf4)

#print(d4.index.is_monotonic_increasing)
#chunksize = 50
print(cdf3._index)
print(ct.row_count)
print(ct)
print(cdf3._table.row_count)
#cdf3._initialize_index(cdf3._index, rows=3)
#print(cdf3._index)
ct.set_index([0,1,2]) #set table index
print(ct.index)
print(ct.iloc[:])
print(ct.iloc[0:1])
cdf3.set_index([0,1,2]) #[0,1,num_rows-1]
print(cdf3._table.index)
print('cdf3 table index:',cdf3.index)
print(cdf3.iloc[0:1])


#print(cdf3._initialize_index(cdf3._index, rows=cdf3._row_count()))
'''
nrows=3
npartitions = 3
chunksize = int(ceil(nrows / npartitions))
locations = list(range(0, nrows, chunksize)) + [len(cdf3)]
divisions = [None] * len(locations)
print(locations, divisions)
print(locations[:-1])
print(locations[1:])

for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:])):
    print(start,stop)
    #print(ct.iloc[start:stop, 'col 0'])
    print('hi')

#print(ct.iloc[0:1, 'col-0'])
'''
#print(ct.dtypes) <- this doesn't work, only added to dataframe
print('cdf3 dtypes:',cdf3.dtypes)

daskcd= dc.from_pycylon(cdf3, npartitions=3, sort=False) #sort set false hence doesn't get divisions #because sort function not implemented in pycylon yet
print('dask cd:',daskcd)
val=[]
val.append(daskcd)
print('dask cd val:',val)
dsk, keys = dask.base._extract_graph_and_keys(val)
print('dsk:',dsk)
print('tasks:',keys)
work = list(set(flatten(keys)))
k=work[0]
print('test:',dsk[k])
print(daskcd.dask)
print(daskcd._name)
print(daskcd._meta)
print(daskcd.divisions)
print(daskcd.npartitions)
#print(daskcd.map_partitions(len)) <- to do
daskcd.visualize(filename='cdf_df.svg')

d4 = {'col-0': [1, 2, 3], 'col-3': [7, 8, 9]}
cdf4 = DataFrame(d4)
cdf4.set_index([0,1,2])
daskcd1= dc.from_pycylon(cdf4, npartitions=3, sort=False)
daskcd1.visualize(filename='cdf_df1.svg')

typ = daskcd1.__class__
print('type of daskcd1:',typ)
for name in ("groupby", "head", "merge", "mean"):
    print(name, ':', hasattr(typ, name))
for name in ("dtypes", "columns"):
    print(name,':',hasattr(daskcd1, name))
for name in ("name", "dtype"):
    print(name,':',hasattr(typ, name))
print('index daskcd:',daskcd.index)
print(daskcd._meta.index)
print(daskcd.index.name)

from pandas.api.types import is_dtype_equal
from pandas.core.dtypes.common import get_dtype

on=['col-0']
left_on =on
right_on=on

print(all(col in daskcd.columns for col in left_on) and all(
        col in daskcd1.columns for col in right_on))

if all(col in daskcd.columns for col in left_on) and all(
        col in daskcd1.columns for col in right_on
):
    dtype_mism = [
        ((lo, ro), daskcd.dtypes[lo], daskcd1.dtypes[ro])
        for lo, ro in zip(left_on, right_on)
        if not is_dtype_equal(daskcd.dtypes[lo], daskcd1.dtypes[ro])
    ]

for lo, ro in zip(left_on,right_on):
    print(lo,ro)
    print(type(lo))
    print(daskcd.dtypes[lo]) #returns value for key lo from the dict returned by pycylon.dtypes
    print(daskcd1.dtypes[ro])
    type1 = daskcd.dtypes[lo]
    type2 = daskcd1.dtypes[ro]
    print('get dtype:', get_dtype(daskcd.dtypes[lo]))
    print('types equal:', is_dtype_equal(type1, type2)) #Type errors returns False
    #print(is_dtype_equal())
    if not is_dtype_equal(daskcd.dtypes[lo], daskcd1.dtypes[ro]):
        print('not equal')
    else:
        print('equal')
print(dtype_mism)
if dtype_mism:
    print('hi')
print('dtypes:',type(daskcd.dtypes['col-0']))
type1 = type(daskcd.dtypes['col-0'])
type1_copy = type(daskcd.dtypes['col-0'])
print('types equal:',is_dtype_equal(type1,type1_copy)) #Type errors returns False
print('dtypes:',type(daskcd1.dtypes['col-0']))
type2 = type(daskcd1.dtypes['col-0'])
print('types equal:',is_dtype_equal(type1,type2))

#print('get dtype:',get_dtype(daskcd.dtypes['col-0']))

merge_df = daskcd.merge(daskcd1,on=['col-0'])
print(merge_df)
merge_df.visualize(filename='cdf_merge.svg')

print(merge_df.compute())


