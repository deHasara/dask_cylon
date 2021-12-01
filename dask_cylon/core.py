import warnings
from distutils.version import LooseVersion
from math import ceil
import pyarrow as pa
import numpy as np
import pandas as pd
from dask.dataframe.io.io import sorted_division_locations
from tlz import partition_all

import dask
from dask import dataframe as dd
from dask.base import normalize_token, tokenize
from dask.compatibility import apply
from dask.context import _globals
from dask.core import flatten
from dask.dataframe.core import Scalar, finalize, handle_out, map_partitions, has_parallel_type, new_dd_object
from dask.dataframe.utils import raise_on_meta_error
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import cull, fuse
from dask.utils import M, OperatorMethodMixin, derived_from, funcname

import pycylon

#from dask_cudf import sorting

DASK_VERSION = LooseVersion(dask.__version__)

def optimize(dsk, keys, **kwargs):
    flatkeys = list(flatten(keys)) if isinstance(keys, list) else [keys]
    dsk, dependencies = cull(dsk, flatkeys)
    #print('dsk inside optimize core:',dsk)
    #print('dependencies:',dependencies)
    dsk, dependencies = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        ave_width=_globals.get("fuse_ave_width", 1),
    )
    dsk, _ = cull(dsk, keys)
    return dsk


class _Frame(dd.core._Frame, OperatorMethodMixin):
    __dask_scheduler__ = staticmethod(dask.get)
    __dask_optimize__ = staticmethod(optimize)

    def __dask_postcompute__(self):
        return finalize, ()

    def __dask_postpersist__(self):
        return type(self), (self._name, self._meta, self.divisions)

    def __init__(self, dsk, name, meta, divisions):
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        self.dask = dsk
        self._name = name
        #print('previous meta:',type(meta), meta)
        meta = dd.core.make_meta(meta)
        #print('meta:',type(meta), meta)
        if not isinstance(meta, self._partition_type):
            raise TypeError(
                f"Expected meta to specify type "
                f"{self._partition_type.__name__}, got type "
                f"{type(meta).__name__}"
            )
        self._meta = meta
        self.divisions = tuple(divisions)

    def __getstate__(self):
        return (self.dask, self._name, self._meta, self.divisions)

    def __setstate__(self, state):
        self.dask, self._name, self._meta, self.divisions = state

    def __repr__(self):
        s = "<dask_cylon.%s | %d tasks | %d npartitions>"
        return s % (type(self).__name__, len(self.dask), self.npartitions)

    def to_dask_dataframe(self, **kwargs):
        """Create a dask.dataframe object from a dask_pycylon object"""
        nullable_pd_dtype = kwargs.get("nullable_pd_dtype", False)
        return self.map_partitions(
            M.to_pandas, nullable_pd_dtype=nullable_pd_dtype
        )

concat = dd.concat

normalize_token.register(_Frame, lambda a: a._name)

class DataFrame(_Frame, dd.core.DataFrame):
    _partition_type = pycylon.DataFrame #changed this from pycylon.Dataframe to pd.DataFrame to use same utils

    def _assign_column(self, k, v):
        def assigner(df, k, v):
            out = df.copy()
            out[k] = v
            return out

        meta = assigner(self._meta, k, dd.core.make_meta(v))
        return self.map_partitions(assigner, k, v, meta=meta)

    def apply_rows(self, func, incols, outcols, kwargs=None, cache_key=None):
        import uuid

        if kwargs is None:
            kwargs = {}

        if cache_key is None:
            cache_key = uuid.uuid4()

        def do_apply_rows(df, func, incols, outcols, kwargs):
            return df.apply_rows(
                func, incols, outcols, kwargs, cache_key=cache_key
            )

        meta = do_apply_rows(self._meta, func, incols, outcols, kwargs)
        return self.map_partitions(
            do_apply_rows, func, incols, outcols, kwargs, meta=meta
        )

    def merge(self, other, **kwargs):
        """shuffle: {'disk', 'tasks'}, optional
            Either ``'disk'`` for single - node operation or ``'tasks'`` for distributed operation.Will be inferred by your current scheduler."""
        if kwargs.pop("shuffle", "tasks") != "tasks":
            raise ValueError(
                "Dask-cudf only supports task based shuffling, got %s"
                % kwargs["shuffle"]
            )
        on = kwargs.pop("on", None)
        if isinstance(on, tuple):
            on = list(on)
        return super().merge(other, on=on, shuffle="tasks", **kwargs) #based on the shuffle type function calls get different, can pass none also(dask)

    def join(self, other, **kwargs):
        if kwargs.pop("shuffle", "tasks") != "tasks":
            raise ValueError(
                "Dask-cudf only supports task based shuffling, got %s"
                % kwargs["shuffle"]
            )

        # CuDF doesn't support "right" join yet
        how = kwargs.pop("how", "left")
        if how == "right":
            return other.join(other=self, how="left", **kwargs)

        on = kwargs.pop("on", None)
        if isinstance(on, tuple):
            on = list(on)
        return super().join(other, how=how, on=on, shuffle="tasks", **kwargs)

##################################################################################

class Series(_Frame, dd.core.Series):
    _partition_type = pycylon.Series

##################################################################################

class Index(Series, dd.core.Index):
    _partition_type = pycylon.indexing.index.BaseArrowIndex

# from_cudf
def from_pycylon(data, npartitions=None, chunksize=None, sort=True, name=None):
    '''
    if isinstance(getattr(data, "index", None), cudf.MultiIndex):
        raise NotImplementedError(
            "dask_cudf does not support MultiIndex Dataframes."
        )
    '''
    name = name or ("from_pycylon-" + tokenize(data, npartitions or chunksize))
    #print(name)
    #print(type(name))
    #print(data)
    #print('initial df:',type(data))
    #print(data._index)
    #print(data.index)
    '''
    return dd.from_pandas( 
        data,
        npartitions=npartitions,
        chunksize=chunksize,
        sort=sort,
        name=name,
    )
    '''
    return from_pandas(              #this is changed because pycylon iloc returned Table and needed to be changed to Dataframe
        data,
        npartitions=npartitions,
        chunksize=chunksize,
        sort=sort,
        name=name,
    )

def from_pandas(data, npartitions=None, chunksize=None, sort=True, name=None):
    """
        Construct a Dask DataFrame from a Pandas DataFrame

        This splits an in-memory Pandas dataframe into several parts and constructs
        a dask.dataframe from those parts on which Dask.dataframe can operate in
        parallel.  By default, the input dataframe will be sorted by the index to
        produce cleanly-divided partitions (with known divisions).  To preserve the
        input ordering, make sure the input index is monotonically-increasing. The
        ``sort=False`` option will also avoid reordering, but will not result in
        known divisions.

        Note that, despite parallelism, Dask.dataframe may not always be faster
        than Pandas.  We recommend that you stay with Pandas for as long as
        possible before switching to Dask.dataframe.

        Parameters
        ----------
        data : pandas.DataFrame or pandas.Series
            The DataFrame/Series with which to construct a Dask DataFrame/Series
        npartitions : int, optional
            The number of partitions of the index to create. Note that depending on
            the size and index of the dataframe, the output may have fewer
            partitions than requested.
        chunksize : int, optional
            The number of rows per index partition to use.
        sort: bool
            Sort the input by index first to obtain cleanly divided partitions
            (with known divisions).  If False, the input will not be sorted, and
            all divisions will be set to None. Default is True.
        name: string, optional
            An optional keyname for the dataframe.  Defaults to hashing the input

        Returns
        -------
        dask.DataFrame or dask.Series
            A dask DataFrame/Series partitioned along the index

        Examples
        --------
        >>> from dask.dataframe import from_pandas
        >>> df = pd.DataFrame(dict(a=list('aabbcc'), b=list(range(6))),
        ...                   index=pd.date_range(start='20100101', periods=6))
        >>> ddf = from_pandas(df, npartitions=3)
        >>> ddf.divisions  # doctest: +NORMALIZE_WHITESPACE
        (Timestamp('2010-01-01 00:00:00', freq='D'),
         Timestamp('2010-01-03 00:00:00', freq='D'),
         Timestamp('2010-01-05 00:00:00', freq='D'),
         Timestamp('2010-01-06 00:00:00', freq='D'))
        >>> ddf = from_pandas(df.a, npartitions=3)  # Works with Series too!
        >>> ddf.divisions  # doctest: +NORMALIZE_WHITESPACE
        (Timestamp('2010-01-01 00:00:00', freq='D'),
         Timestamp('2010-01-03 00:00:00', freq='D'),
         Timestamp('2010-01-05 00:00:00', freq='D'),
         Timestamp('2010-01-06 00:00:00', freq='D'))

        Raises
        ------
        TypeError
            If something other than a ``pandas.DataFrame`` or ``pandas.Series`` is
            passed in.

        See Also
        --------
        from_array : Construct a dask.DataFrame from an array that has record dtype
        read_csv : Construct a dask.DataFrame from a CSV file
        """
    if isinstance(getattr(data, "index", None), pd.MultiIndex):
        raise NotImplementedError("Dask does not support MultiIndex Dataframes.")

    if not has_parallel_type(data):
        raise TypeError("Input must be a pandas DataFrame or Series")

    if (npartitions is None) == (chunksize is None):
        raise ValueError("Exactly one of npartitions and chunksize must be specified.")

    nrows = len(data)

    if chunksize is None:
        chunksize = int(ceil(nrows / npartitions))

    name = name or ("from_pandas-" + tokenize(data, chunksize))

    if not nrows:
        return new_dd_object({(name, 0): data}, name, data, [None, None])

    if sort and not data.index.is_monotonic_increasing:
        data = data.sort_index(ascending=True)
    if sort:
        divisions, locations = sorted_division_locations(
            data.index, chunksize=chunksize
        )
    else:
        locations = list(range(0, nrows, chunksize)) + [len(data)]
        divisions = [None] * len(locations)

    '''
    for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:])):
        print('iloc slicing check:')
        ct = data.iloc[start:stop]
        cdf = pycylon.DataFrame(ct.to_arrow())
        #print(type(data.iloc[start:stop]))
    '''

    dsk = {
        (name, i): pycylon.DataFrame(data.iloc[start:stop].to_arrow())  #for this change had to get this method into dask_cylon/core.py
        for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:]))
    }
    #print("dsk check:",dsk)

    '''
    dsk = {}
    for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:])):
        cdf = pycylon.DataFrame(data.iloc[start:stop].to_arrow())
        cdf.set_index(range(start, stop))
        print('sliced df:', cdf, cdf.index)
        dsk[(name, i)] = cdf  # for this change had to get this method into dask_cylon/core.py

    print("dsk check:", dsk)
    '''
    '''
    dsk = {
        (name, i): data.iloc[start:stop]
        for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:]))
    }
    '''
    return new_dd_object(dsk, name, data, divisions)

from_pycylon.__doc__ = (
    "Wraps main-line Dask from_pandas...\n" + dd.from_pandas.__doc__
)








