import numpy as np
import pandas as pd
from sklearn import base
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from pathlib import Path
from typing import List, Dict

import sys

from hamilton.function_modifiers import extract_columns
from hamilton.function_modifiers import parameterize_values, parameterize_sources, parameterize

cols_string = 'PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked'
cols = cols_string.strip().split(',')
initial_feature_cols = list(set(cols) - set(['Survived']))
cat_cols = ["sex", "cabin", "embarked"]
combined_cat_cols = ["combined_"+e for e in cat_cols]




def ticket_t(
    ticket: pd.Series # raw ticket number
) -> pd.Series: # transformed ticket number
    return ticket.apply(lambda x: str(x).split()[0])

def family(
    sibsp: pd.Series, # number of siblings
    parch: pd.Series # number of parents/children
) -> pd.Series: # number of people in family
    return sibsp + parch

def _label_transformer(
    fit_le: preprocessing.LabelEncoder, # a fit label encoder
    input_series: pd.Series # series to transform 
) -> pd.Series: # transformed series
    return fit_le.transform(input_series)


@parameterize_sources(
    cabin_category = dict(cat='cabin_t', cat_encoder='cabinencoder'),
    sex_category = dict(cat='sex', cat_encoder='sexencoder'),
    embarked_category = dict(cat='embarked', cat_encoder='embarkedencoder')
)
def cat_category(
    cat: pd.Series, # cat series
    cat_encoder: preprocessing.LabelEncoder # fit cat labelencoder
) -> pd.Series: # categorized cat
    return _label_transformer(cat_encoder, cat)

def engineered_features(
    pclass: pd.Series, # passenger class extracted column
    age: pd.Series, # age
    fare: pd.Series, # fare
    cabin_category: pd.Series, # categorical cabin
    sex_category: pd.Series, # categorical sex
    embarked_category: pd.Series, # categorical embarked
    family: pd.Series, # constructed family                
) -> pd.DataFrame: # dataframe with dropped columns ready to feed into model
    df = pd.DataFrame({
        'pclass': pclass,
        'age': age,
        'fare': fare,
        'cabin_category': cabin_category,
        'sex_category': sex_category,
        'embarked_category': embarked_category,
        'family': family
    })
    return df

def final_imputed_features(
    engineered_features: pd.DataFrame, # feature matrix with features dropped
) -> pd.DataFrame: # dataframe with imputed columns ready to feed into model
    return engineered_features.fillna(0)