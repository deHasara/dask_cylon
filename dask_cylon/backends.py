import numpy as np
import pandas as pd
import pyarrow as pa
import pycylon
from pycylon import CylonContext


from dask.dataframe.categorical import categorical_dtype_dispatch
from dask.dataframe.core import get_parallel_type, make_meta, meta_nonempty
from dask.dataframe.extensions import make_array_nonempty
from dask.dataframe.methods import concat_dispatch, tolist_dispatch
from dask.dataframe.utils import (
    UNKNOWN_CATEGORIES,
    _nonempty_scalar,
    _scalar_from_dtype,
    is_arraylike,
    is_scalar, is_integer_na_dtype, is_float_na_dtype,
)

from dask.dataframe.utils import group_split_dispatch, hash_object_dispatch

from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64tz_dtype,
    is_interval_dtype,
    is_period_dtype,
    is_scalar,
    is_sparse,
)

#from dask.dataframe import utils
from dask.dataframe._compat import PANDAS_GT_100, PANDAS_GT_110, PANDAS_GT_120, tm

from .core import DataFrame
from .core import Index

#get_parallel_type.register(pycylon.DataFrame, lambda _: DataFrame)
get_parallel_type.register(pycylon.DataFrame, lambda _: DataFrame)
#get_parallel_type.register(pycylon.Series, lambda _: Series)
#get_parallel_type.register(pycylon.indexing.index.BaseArrowIndex, lambda _: Index)


@meta_nonempty.register(pycylon.indexing.index.BaseArrowIndex)
def _nonempty_index(idx):
    '''
    if isinstance(idx, cudf.core.index.RangeIndex):
        return cudf.core.index.RangeIndex(2, name=idx.name)
    elif isinstance(idx, cudf.core.index.DatetimeIndex):
        start = "1970-01-01"
        data = np.array([start, "1970-01-02"], dtype=idx.dtype)
        values = cudf.core.column.as_column(data)
        return cudf.core.index.DatetimeIndex(values, name=idx.name)
    elif isinstance(idx, cudf.core.index.StringIndex):
        return cudf.core.index.StringIndex(["cat", "dog"], name=idx.name)
    elif isinstance(idx, cudf.core.index.CategoricalIndex):
        key = tuple(idx._data.keys())
        assert len(key) == 1
        categories = idx._data[key[0]].categories
        codes = [0, 0]
        ordered = idx._data[key[0]].ordered
        values = cudf.core.column.build_categorical_column(
            categories=categories, codes=codes, ordered=ordered
        )@meta_nonempty.register(cudf.DataFrame)
        return shufflecudf.core.index.CategoricalIndex(values, name=idx.name)
    elif isinstance(idx, cudf.core.index.GenericIndex):
        return cudf.core.index.GenericIndex(
            np.arange(2, dtype=idx.dtype), name=idx.name
        )
    elif isinstance(idx, cudf.core.MultiIndex):
        levels = [meta_nonempty(lev) for lev in idx.levels]
        codes = [[0, 0] for i in idx.levels]
        return cudf.core.MultiIndex(
            levels=levels, codes=codes, names=idx.names
        )
    '''
    #Need to add based on index type passed
    if isinstance(idx, pycylon.indexing.index.BaseIndex):
        return pycylon.index.RangeIndex(start=0, stop=2)



    raise TypeError(f"Don't know how to handle index of type {type(idx)}")

'''
def _get_non_empty_data(s):
    if isinstance(s._column, cudf.core.column.CategoricalColumn):
        categories = (
            s._column.categories
            if len(s._column.categories)
            else [UNKNOWN_CATEGORIES]
        )
        codes = column.full(size=2, fill_value=0, dtype="int32")
        ordered = s._column.ordered
        data = column.build_categorical_column(
            categories=categories, codes=codes, ordered=ordered
        )
    elif is_string_dtype(s.dtype):
        data = pa.array(["cat", "dog"])
    else:
        if pd.api.types.is_numeric_dtype(s.dtype):
            data = column.as_column(cp.arange(start=0, stop=2, dtype=s.dtype))
        else:
            data = column.as_column(
                cp.arange(start=0, stop=2, dtype="int64")
            ).astype(s.dtype)
    return data

'''
'''
@meta_nonempty.register(cudf.Series)
def _nonempty_series(s, idx=None):
    if idx is None:
        idx = _nonempty_index(s.index)
    data = _get_non_empty_data(s)

    return cudf.Series(data, name=s.name, index=idx)
'''
@meta_nonempty.register(pd.Series)
def _nonempty_series(s, idx=None):
    # TODO: Use register dtypes with make_array_nonempty
    if idx is None:
        idx = _nonempty_index(s.index)
    #print('s:',s)
    #print('s dtype:',s.dtype)
    #print('len s:',len(s))
    dtype = s.dtype
    if len(s) > 0:
        # use value from meta if provided
        data = [s.iloc[0]] * 2
    elif is_datetime64tz_dtype(dtype):
        entry = pd.Timestamp("1970-01-01", tz=dtype.tz)
        data = [entry, entry]
    elif is_categorical_dtype(dtype):
        if len(s.cat.categories):
            data = [s.cat.categories[0]] * 2
            cats = s.cat.categories
        else:
            data = _nonempty_index(s.cat.categories)
            cats = s.cat.categories[:0]
        data = pd.Categorical(data, categories=cats, ordered=s.cat.ordered)
    elif is_integer_na_dtype(dtype):
        data = pd.array([1, None], dtype=dtype)
    elif is_float_na_dtype(dtype):
        data = pd.array([1.0, None], dtype=dtype)
    elif is_period_dtype(dtype):
        # pandas 0.24.0+ should infer this to be Series[Period[freq]]
        freq = dtype.freq
        data = [pd.Period("2000", freq), pd.Period("2001", freq)]
    elif is_sparse(dtype):
        entry = _scalar_from_dtype(dtype.subtype)
        if PANDAS_GT_100:
            data = pd.array([entry, entry], dtype=dtype)
        else:
            data = pd.SparseArray([entry, entry], dtype=dtype)
    elif is_interval_dtype(dtype):
        entry = _scalar_from_dtype(dtype.subtype)
        data = pd.array([entry, entry], dtype=dtype)
    elif type(dtype) in make_array_nonempty._lookup:
        #print('in array nonempty lookup')
        data = make_array_nonempty(dtype)
    else:
        entry = _scalar_from_dtype(dtype)
        #print('entry:',entry) #get dummy data based on type of the column
        data = np.array([entry, entry], dtype=dtype) #fill 2 rows of dummy data

    out = pd.Series(data, name=s.name, index=idx)
    if PANDAS_GT_100:
        out.attrs = s.attrs
    return out


@meta_nonempty.register(pycylon.DataFrame)
def meta_nonempty_dataframe(x):
    #print('got to non empty')
    #print('x:',x)
    #idx = meta_nonempty(x.index)
    idx = pycylon.index.RangeIndex(start=0, stop=2, step=1)
    #print('idx:',idx)
    dt_s_dict = dict()
    data = dict()
    #print('x columns:',x.columns)
    for i, c in enumerate(x.columns):
        #print('i,c:',i,c)
        series = x.iloc[:, i]
        #print('series:',series)
        #print(type(series))
        dt = pa.DataType.to_pandas_dtype(series.to_arrow().schema.types[0]) #table convered to arrow table to get dtypes and convert to pandas dtype
        #print('dt:',dt)
        if dt not in dt_s_dict:
            if dt == np.int64: #need to add more types
                dt_s_dict[dt] = [1,1]
            elif dt == np.object_:
                dt_s_dict[dt] = ['dog','dog']
        data[c] = dt_s_dict[dt]
    #print('dummy data:', data)
    cdf = pycylon.DataFrame(data)
    cdf.set_index([0, 1, 1]) #num_rows-1 -> 2-1 =1
    #print('index dtype :',type(cdf.index.values[0])) <- dtype of the index is same as dtype of the 1st element in the array of index[0,1,1]
    #print('cylon df:',cdf)
    return cdf


@make_meta.register(pycylon.DataFrame)
def make_meta_pycylon(x, index=None):
    #print('X:',x)
    if x:
        #not empty
        ct = x.iloc[0:0]  # returns table (head) #ct = x.iloc[0:0]
        #print('ct type:', type(ct))
        cdf = pycylon.DataFrame(ct.to_arrow())  # get meta as pycylon.DataFrame type
        #print('after conversion: ', type(cdf))
        #print(cdf)
    else:
        cdf =x #when empty just returns the same(df2=df1.head[0] <- df1=df.head[0])

    return cdf
    #return x.iloc[0:0]

@make_meta.register(pycylon.indexing.index.BaseArrowIndex)
def make_meta_pycylon_index(x, index=None):
    return x

def _empty_series(name, dtype, index=None):
    if isinstance(dtype, str) and dtype == "category":
        return pycylon.Series(
            [UNKNOWN_CATEGORIES], dtype=dtype, name=name, index=index
        ).iloc[:0]
    return pycylon.Series([], dtype=dtype, name=name, index=index)


@make_meta.register(object)
def make_meta_object(x, index=None):
    """Create an empty cudf object containing the desired metadata.

    Parameters
    ----------
    x : dict, tuple, list, cudf.Series, cudf.DataFrame, cudf.Index,
        dtype, scalar
        To create a DataFrame, provide a `dict` mapping of `{name: dtype}`, or
        an iterable of `(name, dtype)` tuples. To create a `Series`, provide a
        tuple of `(name, dtype)`. If a cudf object, names, dtypes, and index
        should match the desired output. If a dtype or scalar, a scalar of the
        same dtype is returned.
    index :  cudf.Index, optional
        Any cudf index to use in the metadata. If none provided, a
        `RangeIndex` will be used.

    Examples
    --------
    >>> make_meta([('a', 'i8'), ('b', 'O')])
    Empty DataFrame
    Columns: [a, b]
    Index: []
    >>> make_meta(('a', 'f8'))
    Series([], Name: a, dtype: float64)
    >>> make_meta('i8')
    1
    """
    if hasattr(x, "_meta"):
        return x._meta
    elif is_arraylike(x) and x.shape:
        return x[:0]

    if index is not None:
        #print('not none')
        #print(index, ':', type(index))
        index = make_meta(index)
        #print('check')

    if isinstance(x, dict):
        return pycylon.DataFrame(
            {c: _empty_series(c, d, index=index) for (c, d) in x.items()},
            index=index,
        )
    if isinstance(x, tuple) and len(x) == 2:
        return _empty_series(x[0], x[1], index=index)
    elif isinstance(x, (list, tuple)):
        if not all(isinstance(i, tuple) and len(i) == 2 for i in x):
            raise ValueError(
                f"Expected iterable of tuples of (name, dtype), got {x}"
            )
        return pycylon.DataFrame(
            {c: _empty_series(c, d, index=index) for (c, d) in x},
            columns=[c for c, d in x],
            index=index,
        )
    elif not hasattr(x, "dtype") and x is not None:
        # could be a string, a dtype object, or a python type. Skip `None`,
        # because it is implictly converted to `dtype('f8')`, which we don't
        # want here.
        try:
            dtype = np.dtype(x)
            return _scalar_from_dtype(dtype)
        except Exception:
            # Continue on to next check
            pass

    if is_scalar(x):
        return _nonempty_scalar(x)

    raise TypeError(f"Don't know how to create metadata from {x}")

@concat_dispatch.register(pycylon.DataFrame)
def concat_pycylon(
    dfs,
    axis=0,
    join="outer",
    uniform=False,
    filter_warning=True,
    sort=None,
    ignore_index=False,
):
    assert join == "outer"
    #print('concat dfs:',dfs)#type(dfs[0])
    #print('concated df:',pycylon.frame.concat(dfs, axis=axis))
    return pycylon.frame.concat(dfs, axis=axis)#ignore_index=ignore_index - no ignore_index parameter in cylon concat

try :
    from dask.dataframe.utils import group_split_dispatch, hash_object_dispatch


    @hash_object_dispatch.register(pycylon.frame.DataFrame)
    def hash_object_pycylon(frame, index=True, encoding="utf8", hash_key=None, categorize=True):

        #print('check if get inside hash')
        #print('frame:',frame)
        #print('type frame:',type(frame))
        #print('columns of frame:',frame.columns)
        '''
        if index:
            #print('index yes:',index)
            return safe_hash(frame.reset_index())
        #print('index no')
        return safe_hash(frame)
        '''
        #print('hashed cylon df:',frame.get_hash_object(index=index, encoding=encoding, hash_key=hash_key, categorize=categorize))
        return frame.get_hash_object(index=index, encoding=encoding, hash_key=hash_key, categorize=categorize)


    @group_split_dispatch.register(pycylon.DataFrame)
    def group_split_pycylon(df, c, k, ignore_index=False):
        #print('ignore index:',ignore_index)
        #print('df,c,k:',df,c,k)
        indexer, locations = pd._libs.algos.groupsort_indexer(
            c.astype(np.int64, copy=False), k
        )
        #print('indexer, locations:',indexer,locations)
        df=df.to_pandas()
        df2 = df.take(indexer)
        #print('after indexer:',df2)
        locations = locations.cumsum()
        context = CylonContext(config=None, distributed=False)
        parts = [
            pycylon.DataFrame(pycylon.Table.from_pandas(context,df2.iloc[a:b].reset_index(drop=True))) if ignore_index else pycylon.DataFrame(pycylon.Table.from_pandas(context, df2.iloc[a:b]))
            for a, b in zip(locations[:-1], locations[1:])
        ]
        '''
        for a, b in zip(locations[:-1], locations[1:]):
            print('type:',df2.iloc[a:b], type(df2.iloc[a:b]))
            context = CylonContext(config=None, distributed=False)
            cdf = pycylon.DataFrame(pycylon.Table.from_pandas(context, df2.iloc[a:b]))
            print(cdf)
        '''
        #print('return from group split:',dict(zip(range(k), parts)))
        return dict(zip(range(k), parts))
        '''
        return dict(
            zip(
                range(k),
                df.scatter_by_map(
                    c.astype(np.int32, copy=False),
                    map_size=k,
                    keep_index=not ignore_index,
                ),
            )
        )
        '''
except ImportError:
    pass

'''
try:
    print('check if get here for dispatch')
    from dask.dataframe.utils import group_split_dispatch, hash_object_dispatch

    from cudf.core.column import column

    def safe_hash(frame):
        index = frame.index
        if isinstance(frame, cudf.DataFrame):
            return cudf.Series(frame.hash_columns(), index=index)
        else:
            return cudf.Series(frame.hash_values(), index=index)

    @hash_object_dispatch.register(pycylon.DataFrame)
    def hash_object_pycylon(frame, index=True):
        print('check if get inside hash')
        if index:
            return safe_hash(frame.reset_index())
        return safe_hash(frame)

    @hash_object_dispatch.register(cudf.Index)
    def hash_object_cudf_index(ind, index=None):

        if isinstance(ind, cudf.MultiIndex):
            return safe_hash(ind.to_frame(index=False))

        col = column.as_column(ind)
        return safe_hash(cudf.Series(col))

    @group_split_dispatch.register((cudf.Series, cudf.DataFrame))
    def group_split_cudf(df, c, k, ignore_index=False):
        return dict(
            zip(
                range(k),
                df.scatter_by_map(
                    c.astype(np.int32, copy=False),
                    map_size=k,
                    keep_index=not ignore_index,
                ),
            )
        )


except ImportError:
    pass
'''