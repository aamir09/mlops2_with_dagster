
from dagstermill import define_dagstermill_asset, define_dagstermill_op
from dagstermill import local_output_notebook_io_manager

from dagster import (
    file_relative_path, 
    asset, 
    AssetIn, 
    Out, 
    job, 
    op, 
    In, 
    graph, 
    AssetsDefinition, 
    GraphIn, 
    GraphOut, 
    AssetKey, 
    resource
)

import pandas as pd


import pandas as pd
from upath import UPath

from dagster import (
    Field,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource,
    UPathIOManager,
    io_manager,
    input_manager,
    InputManager,
)


import joblib
import sys

class FixedPathIOManager(UPathIOManager):
    extension: str = ".joblib"

    def _get_path(self, context) -> str:
        context.log.info(context.resource_config)
        context.log.info(type(context))
        return UPath(f"{context.resource_config['base_path']}/{context.name}{FixedPathIOManager.extension}")
    
    def dump_to_path(self, context: OutputContext, obj, path: UPath):
        context.log.info("dump")
        context.log.info(context.resource_config)
        with path.open("wb") as file:
            joblib.dump(obj, file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        context.log.info("load")
        context.log.info(context.resource_config)
        with path.open("rb") as file:
            return joblib.load(file)

    def load_input(self, context):
        context.log.info(">>>>>>>>>>>in load input")
        if context.upstream_output is None: # input manager
            path = self._get_path(context)
        else:
            context.log.info(">>>>>>>>>Using upstream output")
            path = self._get_path(context.upstream_output)
        with path.open("rb") as file:
            return joblib.load(file)



@io_manager(config_schema={"base_path": Field(str, is_required=False)})
def local_model_fixedpath_io_manager(
    init_context: InitResourceContext,
) -> FixedPathIOManager:
    assert init_context.instance is not None  # to please mypy
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return FixedPathIOManager(base_path=base_path)

class FixedPathInputManager(InputManager):
    extension: str = ".joblib"

    def _get_path(self, context) -> str:
        context.log.info(context.resource_config)
        context.log.info(type(context))
        return UPath(f"{context.resource_config['base_path']}/{context.name}{FixedPathIOManager.extension}")
    

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        context.log.info("load")
        context.log.info(context.resource_config)
        with path.open("rb") as file:
            return joblib.load(file)

    def load_input(self, context):
        context.log.info("in load input")
        if context.upstream_output is None: # input manager
            path = self._get_path(context)
        else:
            path = self._get_path(context.upstream_output)
            context.log.info("Using upstream output")
        with path.open("rb") as file:
            return joblib.load(file)

@input_manager(config_schema={"base_path": Field(str, is_required=False)})
def local_model_fixedpath_input_manager(
    init_context: InitResourceContext,
) -> FixedPathInputManager:
    assert init_context.instance is not None  # to please mypy
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return FixedPathInputManager(base_path=base_path)

class PandasParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def _get_path(self, context) -> str:
        context.log.info(context.resource_config)
        context.log.info(type(context))
        return UPath(f"{context.resource_config['base_path']}/{context.name}{PandasParquetIOManager.extension}")

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
        with path.open("wb") as file:
            obj.to_parquet(file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        with path.open("rb") as file:
            return pd.read_parquet(file)

    def load_input(self, context):
        context.log.info("=============================in load input")
        if context.upstream_output is None: # input manager
            path = self._get_path(context)
        else:
            context.log.info("===========================upstream path")
            path = self._get_path(context.upstream_output)
        with path.open("rb") as file:
            return pd.read_parquet(file)

@io_manager(config_schema={"base_path": Field(str, is_required=False)})
def local_pandas_parquet_io_manager(
    init_context: InitResourceContext,
) -> PandasParquetIOManager:
    assert init_context.instance is not None  # to please mypy
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return PandasParquetIOManager(base_path=base_path)



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
def train_type(init_context):
    return "train"

@resource
def test_type(init_context):
    return "test"

@resource
def dataset_type(init_context):
    return "dataset"

@resource
def current_testing_data(init_context):
    return "data/test.csv"

@resource
def current_dataset_data(init_context):
    return "data/test.csv"

from joblib import load
@resource
def encoder_file(init_context):
    return "intermediate_data/encoder.joblib"

@resource
def target_file(init_context):
    return "intermediate_data/target.pkl"

@resource
def train_features_file(init_context):
    return "intermediate_data/featurestore_train.pkl"

@resource
def test_features_file(init_context):
    return "intermediate_data/featurestore_test.pkl"

@resource
def dataset_features_file(init_context):
    return "intermediate_data/featurestore_test.pkl"

@resource
def model_file(init_context):
    return "models/rf.joblib"

def read_data(data: str):
    return pd.read_csv(data)

def read_pickle(data: str):
    return pd.read_pickle(data)


#### OPS ####

@op(required_resource_keys={"data_file"})
def read_data_file(context):
    return read_data(context.resources.data_file)

@op(required_resource_keys={"data_type"})
def read_data_type(context):
    return context.resources.data_type

@op(required_resource_keys={"infer_type"})
def read_infer_type(context):
    return context.resources.infer_type

@op(required_resource_keys={"training_data"})
def read_train_data(context) -> pd.DataFrame:
    return read_data(context.resources.training_data)

@op(required_resource_keys={"testing_data"})
def read_test_data(context) -> pd.DataFrame:
    return read_data(context.resources.testing_data)


@op(required_resource_keys={"encoder_file"})
def read_encoder_file(context) -> dict:
    edict = load(context.resources.encoder_file)
    return edict

@op(required_resource_keys={"target_file"})
def read_target_file(context) -> pd.DataFrame:
    return read_pickle(context.resources.target_file)

@op(required_resource_keys={"train_features_file"})
def read_train_features_file(context) -> pd.DataFrame:
    return read_pickle(context.resources.train_features_file)

@op(required_resource_keys={"inference_features_file"})
def read_features_file(context) -> pd.DataFrame:
    return read_pickle(context.resources.inference_features_file)

@op(required_resource_keys={"model_file"})
def read_model_file(context):
    return load(context.resources.model_file)['fit_clf']


target_extractor_op = define_dagstermill_op(
    name="target_extractor_op",
    notebook_path=file_relative_path(__file__, "../notebooks/target_extractor.ipynb"),
    output_notebook_name="output_target_extractor",
    required_resource_keys={"lake_io_manager"},
    outs={"target": Out(pd.DataFrame, io_manager_key="lake_io_manager")},
    ins={"df_train": In(pd.DataFrame)}
)
#outs={"transformed_data": Out(pd.DataFrame, io_manager_key="lake_io_manager_1")},
encoder_op = define_dagstermill_op(
    name="encoder_op",
    notebook_path=file_relative_path(__file__, "../notebooks/encoder.ipynb"),
    output_notebook_name="output_encoder",
    outs={"encoders": Out(dict, io_manager_key="model_io_manager")},
    ins={"df_train": In(pd.DataFrame), "df_test": In(pd.DataFrame)}
)

@graph(out = {'target': GraphOut()},
)
def target_extractor_graph():
    df_train = read_train_data()
    target, _ = target_extractor_op(df_train)
    return target

@graph(out = {'daencoders': GraphOut()},
)
def encoder_graph():
    df_train = read_train_data()
    df_test = read_test_data()
    daencoders, _ = encoder_op(df_test=df_test, df_train=df_train)
    return daencoders

local_target_extractor_job = target_extractor_graph.to_job(
    name="target_extractor_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "training_data": current_training_data,
        "lake_io_manager": local_pandas_parquet_io_manager,
    }
)

local_encoder_job = encoder_graph.to_job(
    name="local_encoder_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "training_data": current_training_data,
        "testing_data": current_testing_data,
        "model_io_manager": local_model_fixedpath_io_manager,
    }
)

transformer_op = define_dagstermill_op(
    name="transformer_op",
    notebook_path=file_relative_path(__file__, "../notebooks/transform.ipynb"),
    output_notebook_name="output_transform",
    #outs={"transformed_data": Out(pd.DataFrame)},
    required_resource_keys={"lake_io_manager"},
    outs={"transformed_data": Out(pd.DataFrame, io_manager_key="lake_io_manager")},
    ins={"df": In(pd.DataFrame), "encoders": In(dict), "datatype": In(str)}
    ##ins={"df": In(pd.DataFrame), "encoders": In(dict, input_manager_key="model_input_manager"), "datatype": In(str)}
)

@graph(out = {'transformed_data': GraphOut()},
)
def transformer_graph():
    df = read_data_file()
    datatype = read_data_type()
    edict = read_encoder_file()
    transformed_data, _ = transformer_op(datatype = datatype, df = df, encoders = edict)
    #transformed_data, _ = transformer_op(datatype = datatype, df = df)
    return transformed_data


local_train_transformer_job = transformer_graph.to_job(
    name="train_transformer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "data_file": current_training_data, 
        "data_type": train_type,
        "encoder_file": encoder_file,
        #"model_input_manager": local_model_fixedpath_input_manager,
        "lake_io_manager": local_pandas_parquet_io_manager,
    }
)

local_test_transformer_job = transformer_graph.to_job(
    name="test_transformer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "data_file": current_testing_data,
        "data_type": test_type,
        "encoder_file": encoder_file
    }
)

local_dataset_transformer_job = transformer_graph.to_job(
    name="dataset_transformer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "data_file": current_dataset_data,
        "data_type": dataset_type,
        "encoder_file": encoder_file
    }
)

trainer_op = define_dagstermill_op(
    name="trainer_op",
    notebook_path=file_relative_path(__file__, "../notebooks/training.ipynb"),
    output_notebook_name="output_training",
    outs={"training_outputs": Out(dict)},
    ins={"train_features": In(pd.DataFrame), "target": In(pd.DataFrame)}
)

@graph(out = {'output_training': GraphOut()},
)
def trainer_graph():
    target = read_target_file()
    train_features = read_train_features_file()
    training_outputs, _ = trainer_op(target = target, train_features = train_features)
    return training_outputs


local_trainer_job = trainer_graph.to_job(
    name="trainer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "target_file": target_file, 
        "train_features_file": train_features_file
    }
)

inference_op = define_dagstermill_op(
    name="inference_op",
    notebook_path=file_relative_path(__file__, "../notebooks/infer_from_store.ipynb"),
    output_notebook_name="output_infer_from_store",
    outs={"inference_results": Out(dict)},
    ins={"inference_features": In(pd.DataFrame), "fit_clf": In(), "infer_type": In(str)}
)

inference_scratch_op = define_dagstermill_op(
    name="inference_scratch_op",
    notebook_path=file_relative_path(__file__, "../notebooks/infer_from_scratch.ipynb"),
    output_notebook_name="output_infer_from_scratch",
    outs={"inference_results": Out(dict)},
    ins={"df": In(pd.DataFrame), "encoders": In(dict), "datatype": In(str), "fit_clf": In(), "infer_type": In(str)}
)

@graph(out = {'inference_results': GraphOut()},
)
def inference_graph():
    inference_features = read_features_file()
    infer_type = read_infer_type()
    fit_clf = read_model_file()
    inference_results, _ = inference_op(infer_type=infer_type, inference_features = inference_features, fit_clf=fit_clf)
    return inference_results


local_test_inference_job = inference_graph.to_job(
    name="test_inference_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_file": model_file, 
        "inference_features_file": test_features_file,
        "infer_type": test_type
    }
)

local_dataset_inference_job = inference_graph.to_job(
    name="dataset_inference_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_file": model_file, 
        "inference_features_file": dataset_features_file,
        "infer_type": dataset_type
    }
)

@graph(out = {'inference_results': GraphOut()}, 
       ins = {'inference_features': GraphIn()}
)
def inference_from_features_graph(inference_features):
    infer_type = read_infer_type()
    fit_clf = read_model_file()
    inference_results, _ = inference_op(infer_type=infer_type, inference_features = inference_features, fit_clf=fit_clf)
    return inference_results

@graph(out = {'inference_results': GraphOut()})
def inference_from_data_graph():
    transformed_data = transformer_graph()
    inference_results = inference_from_features_graph(inference_features = transformed_data)
    return inference_results


local_inference_from_data_job = inference_from_data_graph.to_job(
    name="inference_from_data_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "data_file": current_dataset_data, 
        "data_type": dataset_type,
        "encoder_file": encoder_file,
        "model_file": model_file, 
        "infer_type": dataset_type
    }
)

@graph(out = {'inference_results': GraphOut()})
def inference_from_data_graph_scratch():
    df = read_data_file()
    datatype = read_data_type()
    edict = read_encoder_file()
    infer_type = read_infer_type()
    fit_clf = read_model_file()
    inference_results, _ = inference_scratch_op(datatype=datatype, df=df, 
                                encoders=edict, infer_type=infer_type, fit_clf=fit_clf)
    return inference_results

local_inference_from_data_job_scratch_config = {

}
local_inference_from_data_job_scratch = inference_from_data_graph_scratch.to_job(
    name="inference_from_data_job_scratch",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "data_file": current_dataset_data, 
        "data_type": dataset_type,
        "encoder_file": encoder_file,
        "model_file": model_file, 
        "infer_type": dataset_type
    }
)