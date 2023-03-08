import pandas as pd
from hamilton import driver, base
from mlops2_with_dagster import encoder_pipeline
from mlops2_with_dagster import features_pipeline
from mlops2_with_dagster import model_pipeline
from pathlib import Path
from mlops2_with_dagster.utils import get_project_dir, printse

index_col = 'passengerid'
target_col = "survived"
cat_cols = ["sex", "cabin", "embarked"]
config = {
    'index_column': index_col,
    'target_column': target_col,
    'categorical_columns': cat_cols
}
config_model = {
    'index_column': index_col,
    'target_column': target_col,
    'random_state': 42,
    'max_depth': None,
    'validation_size_fraction': 0.33,
    't': 0.5
}
config_infer2 = {
    'index_column': index_col,
    'target_column': target_col,
    't': 0.5
}

def encode(df_train: pd.DataFrame, df_test: pd.DataFrame) -> dict:
    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    encode_dr = driver.Driver(config, encoder_pipeline, adapter=adapter)
    output_nodes = ['encoders']
    out = encode_dr.execute(['encoders'],
        inputs = dict(
            df_train = df_train,
            df_test = df_test
        )         
    )
    return out

def target_extract(df_train: pd.DataFrame) -> pd.DataFrame:
    encode_dr = driver.Driver(config, encoder_pipeline)
    output_nodes = ['target']
    out = encode_dr.execute(['target'],
        inputs = dict(
            df_train = df_train        )         
    )
    return out

def transform(df: pd.DataFrame, encoders:dict, datatype:str) -> pd.DataFrame:
    transform_dr = driver.Driver(config, encoder_pipeline, features_pipeline)
    ddf = dict(df = df, **encoders['encoders'])
    output = transform_dr.execute(['final_imputed_features'], inputs = ddf)
    return output

def training(train_feaures:pd.DataFrame, target: pd.DataFrame) -> dict:
    training_adapter = base.SimplePythonGraphAdapter(base.DictResult())
    training_dr = driver.Driver(config_model, 
                           model_pipeline,
                           adapter=training_adapter)
    dtraining = dict(
        final_feature_matrix = train_features,
        target = target.target
    )
    rfdict = training_dr.execute(['fit_clf', 'train_predictions', 'valid_predictions'], inputs = dtraining)
    return rfdict

def infer_from_store(inference_features: pd.DataFrame, fit_clf, infer_type:str) -> dict:
    infer1_adapter = base.SimplePythonGraphAdapter(base.DictResult())
    infer1_dr = driver.Driver(config_model, 
        model_pipeline,
        adapter = infer1_adapter)
    dinfer1 = dict(
        X = inference_features,
        clf = fit_clf
    )
    infer1dict = infer1_dr.execute(['predictions'],inputs = dinfer1)
    return infer1dict

def infer_from_scratch(df: pd.DataFrame, 
                       encoders:dict, 
                       datatype:str,
                       infer_type:str,
                       fit_clf) -> dict:
    infer2_adapter = base.SimplePythonGraphAdapter(base.DictResult())
    infer2_dr = driver.Driver(config_infer2, 
        features_pipeline, model_pipeline, encoder_pipeline,
        adapter = infer2_adapter)
    dinfer2 = dict(
        df = df,
        clf = fit_clf,
        **encoders['encoders']
    )
    infer2dict = infer2_dr.execute(['chain_predictions'],inputs = dinfer2)
    return infer2dict