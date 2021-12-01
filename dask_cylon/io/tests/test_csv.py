import dask
import dask_cylon as dc
import dask.dataframe as dd
import pandas as pd
import numpy as np

file_path = "/home/hasara/Documents/test1.csv"
df = dc.read_csv(file_path,  chunksize="50 B")