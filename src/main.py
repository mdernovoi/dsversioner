from pathlib import Path

import pandas

import dsversioner as dv

#tabular-storage = PostgresTabularStorage(connection string, secrets, user, ...)
#blob-storage = MinioBlobStorage(url, ...)

#storage_context = Storage(TabularStorage, BlobStorage: opt)

# dataset = Dataset("name", storage_context)
# dataset.init(index-col)
# dataset.add(mydataframe)
# dataset.commit()
#
# dataset.add(newdataframe)
# dataset.commit("v147")
#
# dataset2 = Dataset("myname", storage_context)
# dataset2.pull("v1.2.3")
#
# dataset2.add(mynewnwedatafarme)
# dataset2.commit()

def create_example_dataframe_1() -> pandas.DataFrame:

    observation_count = 10

    data = {
        'uri': ['somestring'] * observation_count,
        'image_id': list(range(observation_count)),
        'somevalue': [float(item) for item in list(range(observation_count))],
    }

    df = pandas.DataFrame(data=data)
    df = df.sort_values(by=['image_id'], ascending=False)

    return df


def do_smth():
    filesys_store_ctxt = dv.FileSystemTabularStorage(path=str(Path("dataset_tests")))
    dataset_storage_context = dv.DatasetStorageContext(tabular_storage=filesys_store_ctxt)

    dataset_1 = dv.Dataset(name="ds1", storage_context=dataset_storage_context)
    try:
        dataset_1.init()
    except dv.DatasetExistsException:
        dataset_1.drop()
        dataset_1.init()

    df = create_example_dataframe_1()
    dataset_1.add(df)
    dataset_1.commit()
    dataset_1.commit("v222")
    dataset_1.commit("v222", overwrite_last_commit=True)


if __name__ == '__main__':
    do_smth()