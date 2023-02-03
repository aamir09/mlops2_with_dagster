
from dagstermill import define_dagstermill_asset, define_dagstermill_op
from dagstermill import local_output_notebook_io_manager

from dagster import file_relative_path, asset, AssetIn, Out, job, op, In, graph, AssetsDefinition, GraphIn, GraphOut, AssetKey, resource
import pandas as pd

# input data asset







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
def train_dataset():
    train_data : str = "data/train.csv"
    return read_data(train_data)

@asset
def test_dataset():
    test_data : str = "data/test.csv"
    return read_data(test_data)

input_datasets = [train_dataset, test_dataset]

@resource
def current_training_data(init_context):
    return "data/train.csv"

@resource
def current_testing_data(init_context):
    return "data/test.csv"

def read_data(data: str):
    return pd.read_csv(data)

@op
def read_train_data(context) -> pd.DataFrame:
    return read_data(context.resources.training_data)

@op
def read_test_data() -> pd.DataFrame:
    test_data : str = "data/test.csv"
    return read_data(context.resources.testing_data)

encoder_op = define_dagstermill_op(
    name="encoder_op",
    notebook_path=file_relative_path(__file__, "../notebooks/encoder.ipynb"),
    output_notebook_name="output_encoder",
    outs={"encoders": Out(dict)},
    ins={"df_train": In(pd.DataFrame), "df_test": In(pd.DataFrame)}
)

@graph(out = {'encoders': GraphOut()},
)
def encoder_graph():
    df_train = read_train_data()
    df_test = read_test_data()
    encoders, _ = encoder_op(df_test, df_train)
    return encoders

local_encoder_job = encoder_graph.to_job(
    name="local_encoder_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "training_data": current_training_data,
        "testing_data": current_testing_data
    }
)

transformer_op = define_dagstermill_op(
    name="transformer_op",
    notebook_path=file_relative_path(__file__, "../notebooks/transform.ipynb"),
    output_notebook_name="output_transform",
    outs={"transformed_data": Out(pd.DataFrame)},
    ins={"df": In(pd.DataFrame), "encoders": In(dict), "datatype": In(str)}
)