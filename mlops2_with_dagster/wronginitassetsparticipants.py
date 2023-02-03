
from dagstermill import define_dagstermill_asset, define_dagstermill_op
from dagstermill import local_output_notebook_io_manager

from dagster import file_relative_path, asset, AssetIn, Out, job, op, In, graph, AssetsDefinition, GraphIn, GraphOut, AssetKey, resource
import pandas as pd

# input data asset


def read_data(data: str):
    return pd.read_csv(data)




# get notebook names from targets

# ag = dict(
#     encoder = dict(notebook = "encoder.ipynb",
#                    ins = dict(
#                        df_train=AssetIn("train_dataset"), 
#                        df_test=AssetIn("test_dataset")
#                    )
#     ),
#     trainstore = dict(notebook = "transform.ipynb",
#                       ins = {}
#     ),
#     teststore = dict(notebook = "transform.ipynb",
#                      ins = {}
#     ),
#     training = dict(notebook = "training.ipynb",
#                     ins = {}
#     ),
#     infertest = dict(notebook = "infer_from_store.ipynb",
#                      ins = {}
#     ),
#     inferdata = dict(notebook = "infer_from_scratch.ipynb",
#                      ins = {}
#     ),
# )

notebook_assets = {}
# for key in ag:
#     notebook_assets[key] = define_dagstermill_asset(
#         name=key, 
#         notebook_path=file_relative_path(__file__, f"../notebooks/{ag[key]['notebook']}"),
#         group_name="mlops2",
#         ins=ag[key]['ins']
#     )


@asset
def train_dataset()->pd.DataFrame:
    train_data : str = "data/train.csv"
    return read_data(train_data)

@asset
def test_dataset()->pd.DataFrame:
    test_data : str = "data/test.csv"
    return read_data(test_data)

input_datasets = [train_dataset, test_dataset]

encoder_op = define_dagstermill_op(
    name="encoder_op",
    notebook_path=file_relative_path(__file__, "../notebooks/encoder.ipynb"),
    output_notebook_name="output_encoder",
    outs={"encoders": Out(dict)},
    ins={"df_train": In(pd.DataFrame), "df_test": In(pd.DataFrame)}
)

@graph(out = {'encoders': GraphOut()},
    ins = {'df_train': GraphIn(), 'df_test': GraphIn()}
)
def encoder_graph(df_train, df_test):
    encoders, _ = encoder_op(df_train, df_test)
    return encoders

local_encoder_job = encoder_graph.to_job(
    name="local_encoder_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    },
    input_values={'df_train': AssetKey("train_dataset"), 'df_test': AssetKey("test_dataset")}
)