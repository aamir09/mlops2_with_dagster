# repository.py

from dagster import Definitions, load_assets_from_modules
from dagstermill import local_output_notebook_io_manager

from . import participants

resource_defs = dict(
    output_notebook_io_manager = local_output_notebook_io_manager,
    # lake_io_manager = participants.local_pandas_parquet_io_manager,
    # model_io_manager = participants.local_model_fixedpath_io_manager
)
defs = Definitions(
    assets=participants.input_datasets + list(participants.notebook_assets.values()),
    resources = resource_defs,
    jobs = [participants.local_encoder_job, 
            participants.local_target_extractor_job, 
            participants.local_train_transformer_job, 
            participants.local_test_transformer_job,
            participants.local_dataset_transformer_job,
            participants.local_test_inference_job,
            participants.local_dataset_inference_job,
            participants.local_inference_from_data_job,
            participants.local_inference_from_data_job_scratch,
            participants.local_trainer_job,]
)



            

