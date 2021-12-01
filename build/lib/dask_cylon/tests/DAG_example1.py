import pandas
import dask.dataframe as daskDataFrame

person_IDs = [1,2,3,4,5,6,7,8]
person_last_names = ['Smith', 'Williams', 'Williams','Jackson','Johnson','Smith','Anderson','Christiansen']
person_first_names = ['John', 'Bill', 'Jane','Cathy','Stuart','James','Felicity','Liam']
person_DOBs = ['1982-10-06', '1990-07-04', '1989-05-06', '1974-01-24', '1995-06-05', '1984-04-16', '1976-09-15', '1992-10-02']

peoplePandasDataFrame = pandas.DataFrame({'Person ID':person_IDs,
              'Last Name': person_last_names,
              'First Name': person_first_names,
             'Date of Birth': person_DOBs},
            columns=['Person ID', 'Last Name', 'First Name', 'Date of Birth'])

people_dask_df = daskDataFrame.from_pandas(peoplePandasDataFrame, npartitions=2)
print(people_dask_df.divisions)
print(people_dask_df.npartitions)
print(people_dask_df.map_partitions(len).compute())

person1_IDs = [1,2,3,4,5,6,7,8]
person1_last_names = ['Smith', 'Williams', 'Williams','Jackson','Johnson','Smith','Anderson','Christiansen']
person1_city = ['CA','LA','NY','WA','SD','ND','UT','SF']

peoplePandasDataFrame1 = pandas.DataFrame({'Person ID':person1_IDs,
              'Last Name': person1_last_names,
             'City': person1_city},
            columns=['Person ID', 'Last Name', 'City'])

people_dask_df1 = daskDataFrame.from_pandas(peoplePandasDataFrame1, npartitions=2)
print('index :',peoplePandasDataFrame1.index.name)
from pandas.api.types import is_dtype_equal
on=['Person ID']
left_on =on
right_on=on

print(all(col in people_dask_df.columns for col in left_on) and all(
        col in people_dask_df1.columns for col in right_on))

if all(col in people_dask_df.columns for col in left_on) and all(
        col in people_dask_df1.columns for col in right_on
):
    dtype_mism = [
        ((lo, ro), people_dask_df.dtypes[lo], people_dask_df1.dtypes[ro])
        for lo, ro in zip(left_on, right_on)
        if not is_dtype_equal(people_dask_df.dtypes[lo], people_dask_df1.dtypes[ro])
    ]

for lo, ro in zip(left_on,right_on):
    print(lo,ro)
    print(people_dask_df.dtypes[lo])
    print(people_dask_df1.dtypes[ro])
    #print(is_dtype_equal())
    if not is_dtype_equal(people_dask_df.dtypes[lo], people_dask_df1.dtypes[ro]):
        print('not equal')
    else:
        print('equal')
print(dtype_mism)
if dtype_mism:
    print('hi')
print('dtypes type:',type(people_dask_df.dtypes['Person ID']))

print(people_dask_df1.dask)
print(people_dask_df1.divisions)
print(people_dask_df1.npartitions)
print(people_dask_df1.map_partitions(len).compute())
people_dask_df1.visualize(filename='df.svg')

if __name__ == '__main__':
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)
    client
    merge_df = people_dask_df.merge(people_dask_df1,on=['Person ID'])
    merged_df = merge_df.compute()
    print(merged_df)
    merge_df.visualize(filename='merge22.svg')


