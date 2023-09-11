
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
        if 'file_name' in context.resource_config: # input manager and output with filename
            return UPath(f"{context.resource_config['base_path']}/{context.resource_config['file_name']}")
        else:
            return UPath(f"{context.resource_config['base_path']}/{context.name}{FixedPathIOManager.extension}")
    
    def dump_to_path(self, context: OutputContext, obj, path: UPath):
        context.log.info("dump in fixedpathio")
        context.log.info(context.resource_config)
        with path.open("wb") as file:
            joblib.dump(obj, file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        context.log.info("load in fixedpathio")
        context.log.info(context.resource_config)
        with path.open("rb") as file:
            return joblib.load(file)

    def load_input(self, context):
        context.log.info(f"FPIOxxxxxxxxxxxxx\n {context} AND {context.upstream_output}")
        source_asset = False
        asset_mode = False
        if context.has_asset_key: # this io manager is being used in an asset op
            asset_mode = True
            context.log.info("asset_mode=True")
            context.log.info(context.upstream_output.asset_key.path[0])
            if context.upstream_output.asset_key.path[0][0:7]=='source_':
                source_asset = True
        if (source_asset and asset_mode) or (context.upstream_output is None and 'file_name' in context.resource_config):   # input manager
            # remove because dagstermill processes dont seem to have a context
            # context.log.info(f"{context.metadata}<>{context.name}<>{context.resource_config}")
            # if context.upstream_output is None and 'file_name' in context.resource_config: # input manager
            context.log.info("xxxxxxxxxxx Input Manager Path")
            path = self._get_path(context)
        else:
            context.log.info("xxxxxxxxxxx Upstrem Output Path")
            path = self._get_path(context.upstream_output)
        with path.open("rb") as file:
            return joblib.load(file)



@io_manager(config_schema=
    {
        "base_path": Field(str, is_required=False),
        "file_name": Field(str, is_required=False)
    }
)
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

class PandasParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def _get_path(self, context) -> str:
        context.log.info(f"RESOURCE CONFIG: {context.resource_config}")
        context.log.info(f"CONTEXT TYPE: {type(context)}")
        if 'file_name' in context.resource_config: # input manager and output with filename
            return UPath(f"{context.resource_config['base_path']}/{context.resource_config['file_name']}")
        else:
            return UPath(f"{context.resource_config['base_path']}/{context.name}{PandasParquetIOManager.extension}")

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
        context.log.info("dump in pandasparquet")
        with path.open("wb") as file:
            obj.to_parquet(file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        context.log.info("load in pandasparquet")
        with path.open("rb") as file:
            return pd.read_parquet(file)

    def load_input(self, context):
        context.log.info(f"PPxxxxxxxxxxxxx\n {context} AND {context.upstream_output}")
        source_asset = False
        asset_mode = False
        if context.has_asset_key: # this io manager is being used in an asset op
            asset_mode = True
            context.log.info("asset_mode=True")
            context.log.info(context.asset_key)
            context.log.info(context.upstream_output.asset_key)
            context.log.info(context.upstream_output.asset_key.path[0])
            if context.upstream_output.asset_key.path[0][0:7]=='source_':
                source_asset = True
        if (source_asset and asset_mode) or (context.upstream_output is None and 'file_name' in context.resource_config):   # input manager
            #if context.upstream_output is None and 'file_name' in context.resource_config: # input manager
            context.log.info("xxxxxxxxxxx Input Manager Path")
            path = self._get_path(context)
        else:
            context.log.info("xxxxxxxxxxx Upstrem Output Path")
            bla = context.upstream_output
            context.log.info(dir(bla))
            context.log.info(bla.asset_info)
            path = self._get_path(context.upstream_output)
        with path.open("rb") as file:
            return pd.read_parquet(file)

@io_manager(config_schema=
    {
        "base_path": Field(str, is_required=False),
        "file_name": Field(str, is_required=False)
    }
)
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


class PandasCSVIOManager(UPathIOManager):
    extension: str = ".csv"

    def _get_path(self, context) -> str:
        context.log.info(f"resource_config={context.resource_config}")
        context.log.info(f"type{type(context)}")
        if 'file_name' in context.resource_config: # input manager and output with filename
            return UPath(f"{context.resource_config['base_path']}/{context.resource_config['file_name']}")
        else:
            return UPath(f"{context.resource_config['base_path']}/{context.name}{type(self).extension}")

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
        context.log.info(context.asset_key)
        with path.open("wb") as file:
            obj.to_csv(file)

    def load_from_path(self, context: InputContext, path: UPath) -> pd.DataFrame:
        with path.open("rb") as file:
            return pd.read_csv(file)

    def load_input(self, context):
        context.log.info(f"CSVxxxxxxxxxxxxx\n {context} AND {context.upstream_output}")
        context.log.info(dir(InitResourceContext))
        # remove because dagstermill processes dont seem to have a context
        # context.log.info(f"{context.metadata}<>{context.name}<>{context.resource_config}")
        source_asset = False
        asset_mode = False
        if context.has_asset_key: # this io manager is being used in an asset op
            asset_mode = True
            context.log.info("asset_mode=True")
            context.log.info(context.asset_key)
            context.log.info(context.upstream_output.asset_key)
            context.log.info(context.upstream_output.asset_key.path[0])
            if context.upstream_output.asset_key.path[0][0:7]=='source_':
                source_asset = True
        #if (source_asset and asset_mode) or (context.upstream_output is None and 'file_name' in context.resource_config): # input manager
        if (context.upstream_output is None and 'file_name' in context.resource_config): # input manager
            context.log.info(f"xxxxxxxxxxx Input Manager Path {asset_mode}")
            path = self._get_path(context)
        else:
            context.log.info("xxxxxxxxxxx Upstrem Output Path {asset_mode}")
            bla = context.upstream_output
            context.log.info(dir(bla))
            if asset_mode:
                context.log.info(f"Asset keys {context.asset_key} Upstream {bla.asset_key}")
            path = self._get_path(context.upstream_output)
        #path = self._get_path(context)
        with path.open("rb") as file:
            return pd.read_csv(file)

@io_manager(config_schema=
    {
        "base_path": Field(str, is_required=False),
        "file_name": Field(str, is_required=False)
    }
)
def local_pandas_csv_io_manager(
    init_context: InitResourceContext,
) -> PandasCSVIOManager:
    assert init_context.instance is not None  # to please mypy
    init_context.log.info(f":::{init_context.resource_config}")
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return PandasCSVIOManager(base_path=base_path)

from mlops2_with_dagster.ml_hamilton import (
    encode, 
    target_extract, 
    transform, 
    training, 
    infer_from_store,
    infer_from_scratch
)
from dagster import SourceAsset, multi_asset, AssetOut, define_asset_job


train_data = SourceAsset(key="source_train_dataset", io_manager_key="raw_data_io_manager_train")
test_data = SourceAsset(key="source_test_dataset", io_manager_key="raw_data_io_manager_test")
inference_data = SourceAsset(key="source_inference_dataset", io_manager_key="raw_data_io_manager")


@asset(io_manager_key="model_io_manager_encoders")
def encoders(context, source_train_dataset: pd.DataFrame, source_test_dataset: pd.DataFrame)->dict:
    return encode(source_train_dataset, source_test_dataset)

# resources:
#   lake_io_manager_target:
#     config:
#       base_path: warehouse
#       file_name: target_asset.parquet
#   raw_data_io_manager_train:
#     config:
#       base_path: data
#       file_name: train.csv
@asset(io_manager_key="lake_io_manager_target")
def target(context, source_train_dataset: pd.DataFrame)->pd.DataFrame:
    return target_extract(source_train_dataset)

@asset(io_manager_key="lake_io_manager_target_downstream")
def target_downstream(context, target: pd.DataFrame)->pd.DataFrame:
    return target.copy()

# todo: how to do this separately for training: define a separate op?
@asset(io_manager_key="lake_io_manager")
def dataset_featurestore(context, source_inference_dataset: pd.DataFrame, encoders: dict)->pd.DataFrame:
    # TODO/DONE: need datatype here as a config on the context
    return transform(inference_dataset, encoders, context.op_config['datatype'])

@asset(io_manager_key="lake_io_manager_train")
def train_featurestore(context, source_train_dataset: pd.DataFrame, encoders: dict)->pd.DataFrame:
    return transform(train_dataset, encoders, "train")

@asset(io_manager_key="lake_io_manager_test")
def test_featurestore(context, source_test_dataset: pd.DataFrame, encoders: dict)->pd.DataFrame:
    return transform(train_dataset, encoders, "test")

@asset(io_manager_key="model_io_manager")
def trained(context, train_featurestore: pd.DataFrame, target: pd.DataFrame):
    rdfict = training(train_featurestore, target)
    return rfdict


@asset(io_manager_key="lake_io_manager_predictions")
def dataset_predictions_from_store(context, dataset_featurestore: pd.DataFrame, trained: dict)->pd.DataFrame:
    # TODO/DONE: need infertype here as a config on the context
    fit_clf = trained['fit_clf']
    return infer_from_store(dataset_featurestore, fit_clf, context.op_config['infertype'])

@asset(io_manager_key="lake_io_manager_predictions_test")
def test_predictions(context, test_featurestore: pd.DataFrame, trained: dict)->pd.DataFrame:
    fit_clf = trained['fit_clf']
    return infer_from_store(dataset_featurestore, fit_clf, "test")

@asset(io_manager_key="lake_io_manager_predictions")
def dataset_predictions_from_scratch(context, dataset_featurestore: pd.DataFrame,
                                     encoders: dict,
                                     trained: dict)->pd.DataFrame:
    # TODO/DONE: need datatype and infertype here as a config on the context
    fit_clf = trained['fit_clf']
    return infer_from_scratch(dataset_featurestore, 
                            encoders,
                            context.op_config['datatype'], # datatype
                            context.op_config['infertype'], # infertype
                            fit_clf)


training_only_asset_job = define_asset_job(name="training_only_asset_job", selection=['encoders', 'target', 'train_featurestore', 'trained'])
training_asset_job = define_asset_job(name="training_asset_job", selection=['trained', 'test_predictions'])
target_asset_job = define_asset_job(name="target_asset_job", selection=['target'])
target_asset_job_downstream = define_asset_job(name="target_asset_job_downstream", selection=['target', 'target_downstream'])
inference_as_pipe_asset_job = define_asset_job(name="inference_as_pipe_asset_job", selection=['encoders', 'dataset_featurestore', 'trained', 'dataset_predictions_from_store'])


input_datasets = [train_data, test_data, inference_data]
pipeline_assets = [encoders, target, target_downstream, trained, 
                   dataset_featurestore, train_featurestore, test_featurestore, test_predictions,
                   dataset_predictions_from_scratch, dataset_predictions_from_store]
asset_jobs = [training_only_asset_job, training_asset_job, target_asset_job, target_asset_job_downstream, inference_as_pipe_asset_job]

asset_resource_defs = dict(
    output_notebook_io_manager = local_output_notebook_io_manager,
    lake_io_manager = local_pandas_parquet_io_manager,
    lake_io_manager_target = local_pandas_parquet_io_manager,
    lake_io_manager_target_downstream = local_pandas_parquet_io_manager,
    lake_io_manager_train = local_pandas_parquet_io_manager,
    lake_io_manager_test = local_pandas_parquet_io_manager,
    lake_io_manager_predictions_test = local_pandas_parquet_io_manager,
    lake_io_manager_predictions = local_pandas_parquet_io_manager,
    model_io_manager = local_model_fixedpath_io_manager,
    model_io_manager_encoders = local_model_fixedpath_io_manager,
    raw_data_io_manager = local_pandas_csv_io_manager,
    raw_data_io_manager_train = local_pandas_csv_io_manager,
    raw_data_io_manager_test = local_pandas_csv_io_manager,
)


target_extractor_op = define_dagstermill_op(
    name="target_extractor_op",
    notebook_path=file_relative_path(__file__, "../notebooks/target_extractor_modified.ipynb"),
    output_notebook_name="output_target_extractor",
    outs={"target": Out(pd.DataFrame, io_manager_key="lake_io_manager_target")},
    ins={"df_train": In(pd.DataFrame, input_manager_key="raw_data_input_manager_train")}
)
# @graph(
#     out = {'target': GraphOut("The extracted Target Column")},
#     ins = {'df_train': GraphIn("The training set containing the Target")}
# )

@graph
def target_extractor_graph():
    #df_train = read_train_data()
    #target, _ = target_extractor_op(df_train)
    target, _ = target_extractor_op()
    return target

local_target_extractor_job = target_extractor_graph.to_job(
    name="target_extractor_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        # "training_data": current_training_data,
        "lake_io_manager_target": local_pandas_parquet_io_manager,
        "raw_data_input_manager_train": local_pandas_csv_io_manager,
    }
)


encoder_op = define_dagstermill_op(
    name="encoder_op",
    notebook_path=file_relative_path(__file__, "../notebooks/encoder_modified.ipynb"),
    output_notebook_name="output_encoder",
    outs={"encoders": Out(dict, io_manager_key="model_io_manager_encoder")},
    ins={"df_train": In(pd.DataFrame, input_manager_key="raw_data_input_manager_train"), 
         "df_test": In(pd.DataFrame, input_manager_key="raw_data_input_manager_test")
    }
)


@graph(out = {'daencoders': GraphOut()},
)
def encoder_graph():
    # df_train = read_train_data()
    # df_test = read_test_data()
    daencoders, _ = encoder_op()
    return daencoders

local_encoder_job = encoder_graph.to_job(
    name="local_encoder_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        # "training_data": current_training_data,
        # "testing_data": current_testing_data,
        "model_io_manager_encoder": local_model_fixedpath_io_manager,
        "raw_data_input_manager_train": local_pandas_csv_io_manager,
        "raw_data_input_manager_test": local_pandas_csv_io_manager,
    }
)

transformer_op = define_dagstermill_op(
    name="transformer_op",
    notebook_path=file_relative_path(__file__, "../notebooks/transform_modified.ipynb"),
    output_notebook_name="output_transform",
    outs={"transformed_data": Out(pd.DataFrame, io_manager_key="lake_io_manager")},
    #ins={"df": In(pd.DataFrame), "encoders": In(dict), "datatype": In(str)}
    ins={"df": In(pd.DataFrame, input_manager_key="raw_data_input_manager"), 
         "encoders": In(dict, input_manager_key="model_input_manager"), 
    },
    config_schema={
        "datatype" : Field(str, is_required=True)
    }
)

@graph(out = {'transformed_data': GraphOut()},
)
def transformer_graph():
    transformed_data, _ = transformer_op()
    return transformed_data


local_train_transformer_job = transformer_graph.to_job(
    name="train_transformer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_input_manager": local_model_fixedpath_io_manager,
        "lake_io_manager": local_pandas_parquet_io_manager,
        "raw_data_input_manager": local_pandas_csv_io_manager,
    }
)

local_test_transformer_job = transformer_graph.to_job(
    name="test_transformer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_input_manager": local_model_fixedpath_io_manager,
        "lake_io_manager": local_pandas_parquet_io_manager,
        "raw_data_input_manager": local_pandas_csv_io_manager,
    }
)

local_dataset_transformer_job = transformer_graph.to_job(
    name="dataset_transformer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_input_manager": local_model_fixedpath_io_manager,
        "lake_io_manager": local_pandas_parquet_io_manager,
        "raw_data_input_manager": local_pandas_csv_io_manager,
    }
)

trainer_op = define_dagstermill_op(
    name="trainer_op",
    notebook_path=file_relative_path(__file__, "../notebooks/training_modified.ipynb"),
    output_notebook_name="output_training",
    outs={"training_outputs": Out(dict, io_manager_key="model_io_manager_clf")},
    ins={"train_features": In(pd.DataFrame, input_manager_key="lake_input_manager_features"), 
         "target": In(pd.DataFrame, input_manager_key="lake_input_manager_target")}
)

@graph(out = {'output_training': GraphOut()},
)
def trainer_graph():
    training_outputs, _ = trainer_op()
    return training_outputs


local_trainer_job = trainer_graph.to_job(
    name="trainer_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "lake_input_manager_target": local_pandas_parquet_io_manager,
        "lake_input_manager_features": local_pandas_parquet_io_manager,
        "model_io_manager_clf": local_model_fixedpath_io_manager
    }
)


@graph(out = {'training_outputs': GraphOut()})
def training_from_data_graph():
    target, _ = target_extractor_op()
    encoders, _ = encoder_op()
    transformed_data, _ = transformer_op(encoders=encoders)
    training_outputs = trainer_op(train_features = transformed_data, target=target)
    return training_outputs

# config has datatype and infertype
local_training_from_data_job = training_from_data_graph.to_job(
    name="training_from_data_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "lake_io_manager_target": local_pandas_parquet_io_manager, # target
        "raw_data_input_manager_train": local_pandas_csv_io_manager, # target
        "model_io_manager_encoder": local_model_fixedpath_io_manager, # encoder
        "raw_data_input_manager_test": local_pandas_csv_io_manager, # encoder
        "model_input_manager": local_model_fixedpath_io_manager, # transformer
        "raw_data_input_manager": local_pandas_csv_io_manager, # transformer
        "lake_io_manager": local_pandas_parquet_io_manager, # transformer
        "lake_input_manager_features": local_pandas_parquet_io_manager, # trainer
        "lake_input_manager_target": local_pandas_parquet_io_manager, # trainer
        "model_io_manager_clf": local_model_fixedpath_io_manager, # trainer
    }
)

inference_store_op = define_dagstermill_op(
    name="inference_store_op",
    notebook_path=file_relative_path(__file__, "../notebooks/infer_from_store.ipynb"),
    output_notebook_name="output_infer_from_store",
    outs={"inference_results": Out(dict, io_manager_key="model_io_manager")},
    ins={"inference_features": In(pd.DataFrame, input_manager_key="lake_input_manager_features"), 
         "clfinfo": In(dict, input_manager_key="model_input_manager_clf"), 
    },
    config_schema={
        "infertype" : Field(str, is_required=True)
    }
)



@graph(out = {'inference_results': GraphOut()},
)
def inference_graph():
    # inference_features = read_features_file()
    # infer_type = read_infer_type()
    # fit_clf = read_model_file()
    inference_results, _ = inference_store_op()
    return inference_results


local_test_inference_job = inference_graph.to_job(
    name="test_inference_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_io_manager": local_model_fixedpath_io_manager,
        "model_input_manager_clf": local_model_fixedpath_io_manager,
        "lake_input_manager_features": local_pandas_parquet_io_manager,
    }
)

local_dataset_inference_job = inference_graph.to_job(
    name="dataset_inference_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_io_manager": local_model_fixedpath_io_manager,
        "model_input_manager_clf": local_model_fixedpath_io_manager,
        "lake_input_manager_features": local_pandas_parquet_io_manager,
    }
)

inference_scratch_op = define_dagstermill_op(
    name="inference_scratch_op",
    notebook_path=file_relative_path(__file__, "../notebooks/infer_from_scratch_modified.ipynb"),
    output_notebook_name="output_infer_from_scratch",
    outs={"inference_results": Out(dict, io_manager_key="model_io_manager")},
    ins={"df": In(pd.DataFrame, input_manager_key="raw_data_input_manager"), 
         "encoders": In(dict, input_manager_key="model_input_manager_encoder"), 
         "clfinfo": In(dict, input_manager_key="model_input_manager_clf")
    },
    config_schema={
        "infertype" : Field(str, is_required=True)
    }
)

@graph(out = {'inference_results': GraphOut()})
def inference_from_data_scratch_graph():
    # df = read_data_file()
    # datatype = read_data_type()
    # edict = read_encoder_file()
    # infer_type = read_infer_type()
    # fit_clf = read_model_file()
    inference_results, _ = inference_scratch_op()
    return inference_results


local_inference_from_data_scratch_job = inference_from_data_scratch_graph.to_job(
    name="inference_from_data_scratch_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_input_manager_clf": local_model_fixedpath_io_manager,
        "model_input_manager_encoder": local_model_fixedpath_io_manager,
        "model_io_manager": local_model_fixedpath_io_manager,
        "raw_data_input_manager": local_pandas_csv_io_manager,
    }
)


@graph(out = {'inference_results': GraphOut()}, 
       ins = {'inference_features': GraphIn()}
)
def inference_from_features_graph(inference_features):
    # infer_type = read_infer_type()
    # fit_clf = read_model_file()
    inference_results, _ = inference_store_op(inference_features=inference_features)
    return inference_results

@graph(out = {'inference_results': GraphOut()})
def inference_from_data_graph():
    transformed_data = transformer_graph()
    inference_results = inference_from_features_graph(inference_features = transformed_data)
    return inference_results

# config has datatype and infertype
local_inference_from_data_job = inference_from_data_graph.to_job(
    name="inference_from_data_job",
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "model_input_manager": local_model_fixedpath_io_manager,
        "model_input_manager_clf": local_model_fixedpath_io_manager,
        "model_io_manager": local_model_fixedpath_io_manager,
        "raw_data_input_manager": local_pandas_csv_io_manager,
        "lake_io_manager": local_pandas_parquet_io_manager,
        "lake_input_manager_features": local_pandas_parquet_io_manager,
    }
)

op_based_jobs = [local_encoder_job, 
            local_target_extractor_job, 
            local_train_transformer_job, 
            local_test_transformer_job,
            local_dataset_transformer_job,
            local_test_inference_job,
            local_dataset_inference_job,
            local_inference_from_data_job, # makes features, then runs inference: two connected graphs
            local_inference_from_data_scratch_job, # all the code is in one notebook
            local_trainer_job,
            local_training_from_data_job]
# now let us define a sensor to connect two jobs

from dagster import (
    DagsterRunStatus,
    run_status_sensor,
    RunRequest,
    sensor
)

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=local_dataset_inference_job,
    monitored_jobs=[local_dataset_transformer_job]
)
def do_inference_from_featurestore_sensor(context):
    output_parquet = context.dagster_run.run_config['resources']['lake_io_manager']['config']['file_name']
    context.log.info(output_parquet)
    output_root = output_parquet.split('.')[0] # todo: make it everything not -1 or pass explicit config keys as metadata
    run_config = {
        'ops': {'inference_store_op': {'config': {'infertype': 'dataset'}}},
        'resources': {'lake_input_manager_features': {'config': {'base_path': 'warehouse',
                                                          'file_name': output_parquet}},
               'model_input_manager_clf': {'config': {'base_path': 'warehouse',
                                                  'file_name': 'rf.joblib'}},
               'model_io_manager': {'config': {'base_path': 'results',
                                               'file_name': f"{output_root}.joblib"}}}
    }
    return RunRequest(run_key=None, run_config=run_config)

import os

MONITORED_FOLDER_INFERENCE = "incoming/inference"
@sensor(job=local_dataset_transformer_job)
def new_data_sensor(context):
    last_mtime = float(context.cursor) if context.cursor else 0
    max_mtime = last_mtime
    for filename in os.listdir(MONITORED_FOLDER_INFERENCE):
        #filename = NEW_DATA
        fileroot = filename.split('.')[0] # split bla.csv
        filepath = os.path.join(MONITORED_FOLDER_INFERENCE, filename)
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime
            if file_mtime >= last_mtime:
                yield RunRequest(
                    run_key=f"{filename}:{str(file_mtime)}",
                    run_config={
                        'ops': {'transformer_op': {'config': {'datatype': 'dataset'}}},
                                'resources': {
                                    'lake_io_manager': {'config': {'base_path': 'warehouse',
                                                'file_name': f"featurestore_{fileroot}.parquet"}},
                                    'model_input_manager': {'config': {'base_path': 'warehouse',
                                                    'file_name': 'encoders.joblib'}},
                                    'raw_data_input_manager': {'config': {'base_path': 'incoming/inference',
                                                        'file_name': filename}}}
                    },
                )
            max_mtime = max(max_mtime, file_mtime)
    context.update_cursor(str(max_mtime))

MONITORED_FOLDER_TRAINING = "incoming/training"
@sensor(job=local_training_from_data_job)
def new_training_sensor(context):
    last_mtime = float(context.cursor) if context.cursor else 0
    max_mtime = last_mtime
    for filename in os.listdir(MONITORED_FOLDER_TRAINING):
        #filename = NEW_DATA
        fileroot = filename.split('.')[0] # split bla.csv
        filepath = os.path.join(MONITORED_FOLDER_TRAINING, filename)
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime
            if file_mtime >= last_mtime:
                yield RunRequest(
                    run_key=f"{filename}:{str(file_mtime)}",
                    run_config={
                        'ops': {'transformer_op': {'config': {'datatype': 'train'}}},
                        'resources': {'lake_input_manager_features': {'config': {'base_path': 'warehouse'}},
                                        'lake_input_manager_target': {'config': {'base_path': 'warehouse'}},
                                        'lake_io_manager': {'config': {'base_path': 'warehouse',
                                                                        'file_name': 'featurestore_train.parquet'}},
                                        'lake_io_manager_target': {'config': {'base_path': 'warehouse',
                                                                                'file_name': 'target.parquet'}},
                                        'model_input_manager': {'config': {'base_path': 'warehouse'}},
                                        'model_io_manager_clf': {'config': {'base_path': 'warehouse',
                                                                            'file_name': 'rf.joblib'}},
                                        'model_io_manager_encoder': {'config': {'base_path': 'warehouse',
                                                                                'file_name': 'encoders.joblib'}},
                                        'raw_data_input_manager': {'config': {'base_path': 'incoming/training',
                                                                                'file_name': filename}},
                                        'raw_data_input_manager_test': {'config': {'base_path': 'data',
                                                                                    'file_name': 'test.csv'}},
                                        'raw_data_input_manager_train': {'config': {'base_path': 'incoming/training',
                                                                                    'file_name': filename}}
                                    }
                    },
                )
            max_mtime = max(max_mtime, file_mtime)
    context.update_cursor(str(max_mtime))