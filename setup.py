from setuptools import find_packages, setup

setup(
    name="mlops2_with_dagster",
    packages=find_packages(exclude=["mlops2_with_dagster_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
