"""This is the code for SNSE parallel"""

from kfp import dsl
from kfp.dsl import (
    Artifact,
    Dataset,
    Input,
    Model,
    Output,
    Metrics,
    Markdown,
    HTML,
    component, 
    OutputPath, 
    InputPath,
    pipeline
)
from kfp import compiler
from google.cloud.aiplatform import pipeline_jobs
from google.cloud import aiplatform

import datetime as dt

from vertex_configconfig import (
    PROJECT_ID,
    BASE_IMAGE,
    BUCKET_PATH,
    REGION,
    SERVICE_ACCOUNT
)

# Experiment setup
# inx = 0
# inx = 1
# inx = 2
inx = 3

exp_group = [25, 50, 100, 250]
exp = exp_group[inx]
exp_case = 'SNSE'
exp_type = "parallel"
prediction_type = "regression"


@dsl.component(
    base_image=BASE_IMAGE,
    output_component_file="model.yaml"
)
def scaling_tpot(
    output_dir: Output[Artifact],
    input_url: str,
    entity_partition_input: str
):  
    from deltalake import DeltaTable, write_deltalake
    from pathlib import Path
    import pandas as pd
    import polars as pl
    from tpot import TPOTClassifier, TPOTRegressor
    from sklearn.model_selection import train_test_split
    
    sample_fraction = 1.0
    entity_partition = entity_partition_input
    
    tpot = TPOTRegressor(
        generations=5, 
        population_size=50, 
        verbosity=2, 
        random_state=42, 
        n_jobs=-1, 
        config_dict='TPOT light'
    )

    entity_pldf = pl.read_delta(input_url, pyarrow_options={"partitions": [("entity", "=", f"{entity_partition}")]})
    entity_pdf = entity_pldf.sample(fraction=sample_fraction).to_pandas()

    features = [s for s in entity_pdf.columns.to_list() if s.isdigit()]
    target = 'target'

    # prepare data
    X_train, X_test, y_train, y_test = train_test_split(
        entity_pdf[features], entity_pdf[target], train_size=0.75, test_size=0.25, random_state=42
    )

    print(entity_pdf.shape, X_train.shape, y_train.shape)
    tpot.fit(X_train, y_train)
    
    print("Best score:", tpot.score(X_test, y_test), "\n")
    print("-----------------------------------------------")
    print("pareto_front_fitted_pipelines_\n")
    print(tpot.pareto_front_fitted_pipelines_)
    print("-----------------------------------------------")
    print("evaluated_individuals_\n")
    print(tpot.evaluated_individuals_)
    print("-----------------------------------------------")
    print("The selected pipeline\n")
    print(tpot.fitted_pipeline_)
    
    
    tpot.export(output_dir.path)
    
    
# Pipeline setup
BUCKET_NAME_prefix = "gs://"
FUSE_BUCKET_prefix = "/gcs/"
API_ENDPOINT = f"{REGION}-aiplatform.googleapis.com"
vertex_exp_name = f"case_{exp_case}-{exp_type}-group{exp}-{prediction_type}"
BUCKET_PATH_EXP_GROUP = f"{BUCKET_PATH}/{exp}/{prediction_type}"
BUCKET_NAME = BUCKET_NAME_prefix + BUCKET_PATH_EXP_GROUP
FUSE_BUCKET = FUSE_BUCKET_prefix + BUCKET_PATH_EXP_GROUP

# USE TIMESTAMP TO DEFINE UNIQUE PIPELINE NAMES
TIMESTAMP = dt.datetime.now().strftime("%Y%m%d%H%M%S")
PIPELINE_ROOT = f"{BUCKET_NAME_prefix}{BUCKET_PATH}/{exp}/{vertex_exp_name}_pipeline_root/"
INPUT_URL = f"{FUSE_BUCKET}/"

aiplatform.init(
    experiment=vertex_exp_name,
    experiment_description=f"Run single node single entity sequentially",
    experiment_tensorboard=False,
    project=PROJECT_ID,
    location=REGION,
)

for i in range(exp):
    INPUT_ENTITY = f"entity_{i}"
    DISPLAY_NAME = f"scaling-tpot-case-{exp_case}-group{exp}-{INPUT_ENTITY}"
    PACKAGE_PATH = f"json/{DISPLAY_NAME}.json"

    # Define the pipeline. Notice how steps reuse outputs from previous steps
    @dsl.pipeline(
        pipeline_root=PIPELINE_ROOT,
        name=DISPLAY_NAME   
    )
    def pipeline(
        project: str = PROJECT_ID,
        region: str = REGION, 
        display_name: str = DISPLAY_NAME,
        input_url: str = INPUT_URL,
        entity_partition_input: str = INPUT_ENTITY
    ):

        task = scaling_tpot(input_url=input_url, entity_partition_input=entity_partition_input)

    # Compile the pipeline as JSON
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=PACKAGE_PATH
    )

    # Start the pipeline
    start_pipeline = pipeline_jobs.PipelineJob(
        display_name=DISPLAY_NAME,
        template_path=PACKAGE_PATH,
        enable_caching=False,
        location=REGION,
        project=PROJECT_ID
    )

    # Run the pipeline
    # start_pipeline.run(service_account=SERVICE_ACCOUNT)
    start_pipeline.submit(service_account=SERVICE_ACCOUNT, experiment=vertex_exp_name)
