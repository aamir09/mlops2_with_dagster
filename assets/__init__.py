
from dagstermill import define_dagstermill_asset
from dagster import file_relative_path

# get notebook names from targets

notebooks = dict(
    encoder = "encoder.ipynb",
    trainstore = "transform_train.ipynb",
    teststore = "transform_test.ipynb",
    training = "training.ipynb",
    infertest = "infer_from_store.ipynb",
    inferdata = "infer_from_scratch.ipynb"
)

targets = {}
for key in notebooks:
    targets[key] = define_dagstermill_asset(
        name=key, 
        notebook_path=file_relative_path(__file__, f"../{notebooks[key]}"),
        group_name="mlops2",
    )
