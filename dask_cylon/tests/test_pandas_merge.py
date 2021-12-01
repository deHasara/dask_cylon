import numpy as np
import pandas as pd
from pandas.core.reshape.merge import _MergeOperation, _any

#df0 = {'lcol-0': list(range(100)), 'col-1': list(np.random.randint(low = 3,high=1000,size=100))}
df0 = {'col-0': list(range(100)), 'col-1': list(np.random.randint(low = 3,high=1000,size=100))}
pdf = pd.DataFrame(df0)

#df1 = {'rcol-0': list(range(1000)), 'col-1':list(np.random.randint(low = 3,high=1000,size=1000))}
df1 = {'col-0': list(range(1000)), 'col-1':list(np.random.randint(low = 3,high=1000,size=1000))}
pdf1 = pd.DataFrame(df1)

#print(pdf.index.intersection(pdf1.index).names)



left = pdf
right = pdf1
how: str = "inner"
on=None
left_on='col-0'#'lcol-0'
right_on='col-0'#'rcol-0'
left_index: bool = False
right_index: bool = False
sort: bool = False
suffixes=("_x", "_y")
copy: bool = True
indicator: bool = False
validate=None

print(left_on)
merge = pd.core.frame.DataFrame.merge(
            left,
            right,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_index=left_index,
            right_index=right_index,
            sort=sort,
            suffixes=suffixes,
            copy=copy,
            indicator=indicator,
            validate=validate,
)

#print(merge.left_on)
op = _MergeOperation(
        left,
        right,
        how=how,
        on=on,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        sort=sort,
        suffixes=suffixes,
        copy=copy,
        indicator=indicator,
        validate=validate,
    )
#print(op)
print(_any(op.left_on))
print(op._cross)
#kwargs ={'how': 'inner', 'left_on': ['col-0'], 'right_on': ['col-0'], 'left_index': False, 'right_index': False, 'suffixes': ('_x', '_y'), 'indicator': False}

#print(pdf.merge(pdf1, left_on='lcol-0', right_on='rcol-0'))
print(pdf.merge(pdf1, left_on='col-0', right_on='col-0'))