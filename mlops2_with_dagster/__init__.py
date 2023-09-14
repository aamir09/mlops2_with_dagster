# repository.py

from dagster import Definitions

from . import participants


defs = Definitions(
    assets=participants.input_datasets + participants.pipeline_assets,
    resources = participants.asset_resource_defs,
    jobs = participants.asset_jobs + participants.op_based_jobs,
    sensors = [participants.do_inference_from_featurestore_sensor,
               participants.new_data_sensor, participants.new_training_sensor,
               participants.train_on_new_data_sensor]
)



            

