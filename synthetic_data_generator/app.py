import typer
import pandas as pd
from sklearn.datasets import make_classification, make_regression
from typing_extensions import Annotated
import pathlib
from pathlib import Path
from configparser import ConfigParser
import random
from rich.progress import track
import logging
from rich.logging import RichHandler
from synthetic_data_generator.streaming.sdg_producer import send_to_kafka ,create_topic
from enum import Enum


FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")


app = typer.Typer()

TABLE_PATH = Path("./data/delta")

# Range of rows for 80-20 split
N_80_RANGE = {"start": 100_00, "end": 1_000_00}
N_20_RANGE = {"start": 1_000_00, "end": 1_500_00}
# config file to store entity and num of rows mapping
CONFIG_FILE_PATH = pathlib.Path("./conf/config.ini")
N_FEATURES = 20
TOPIC_NAME = "entity"




class Sink(str,Enum):
    delta = "delta"
    kafka = "kafka"
    none = "none"

class GenType(str, Enum):
    classification = "classification"
    regression = "regression"



def sink_to_deltalake(df: pd.DataFrame):
    from deltalake import write_deltalake

    write_deltalake(
        table_or_uri=TABLE_PATH, data=df, partition_by=["entity"], mode="append"
    )


@app.command()
def read_config():
    if not CONFIG_FILE_PATH.exists():
        raise FileNotFoundError(f"config file not found at {CONFIG_FILE_PATH}")
    config = ConfigParser()
    config.read(CONFIG_FILE_PATH)
    for entity in config.sections():
        logger.info(entity)
        for n_samples, value in config.items(entity):
            logger.info(n_samples)
            logger.info(value)


@app.command()
def create_config(
    n_entity: int = 20,
):
    TABLE_PATH.mkdir(parents=True, exist_ok=True)

    # split 80 to 20

    n_80 = int(0.80 * n_entity)
    n_20 = n_entity - n_80
    print(f"n_80 {n_80} and n_20 {n_20}")

    config = ConfigParser()

    for i in range(n_80):

        # Add sections and key-value pairs
        config[f"entity_{i}"] = {
            "n_samples": random.randint(N_80_RANGE["start"], N_80_RANGE["end"])
        }

    for i in range(n_20):
        config[f"entity_{i+n_80}"] = {
            "n_samples": random.randint(N_20_RANGE["start"], N_20_RANGE["end"])
        }

    # Write the configuration to a file

    with open(CONFIG_FILE_PATH, "w") as config_file:
        logger.debug(
            f"Creating config file at {CONFIG_FILE_PATH} for {n_entity} entities"
        )
        config.write(config_file)


def generate_regression_data(entity: int, n_samples: int, sink_type: Sink):
    """Generate regression data using make_regression from sklearn

    Args:
        entity: no of entities
        n_samples: no of samples
        sink_type: destination to write data
    """

    logger.debug(n_samples)

    X, y = make_regression(
        n_samples=int(n_samples),
        n_features=int(N_FEATURES),
    )

    df = pd.DataFrame(X)
    df["target"] = y
    df["entity"] = entity

    if sink_type == "delta":
        sink_to_deltalake(df=df)
    elif sink_type == "kafka":
        send_to_kafka(topic_name=TOPIC_NAME,df=df, key=str(entity))       


def generate_classification_data(entity: int, n_samples: int, sink_type: Sink):
    """Generate classification data using make_classification from sklearn

    Args:
        entity: no of entities
        n_samples: no of samples
        sink_type: destination to write data
    """

    logger.debug(n_samples)

    X, y = make_classification(
        n_samples=int(n_samples),
        n_features=int(N_FEATURES),
    )

    df = pd.DataFrame(X)
    df["target"] = y
    df["entity"] = entity

    if sink_type == "delta":
        sink_to_deltalake(df=df)
    elif sink_type == "kafka":
        send_to_kafka(topic_name=TOPIC_NAME,df=df, key=str(entity))


@app.command()
def generate_data( n_entity: int=20,
    sink_type: Sink = Sink.none,
    gen_type: GenType = GenType.classification,
):
    """Generate data and write to the selected sink option.

    Args:
        n_entity: no of entities
        sink_type: write destination. Defaults to Sink.none.
        gen_type: type data to be generated. Defaults to GenType.classification.
    """

    if not CONFIG_FILE_PATH.exists():
        create_config(n_entity=n_entity)
    
    if sink_type == Sink.kafka:
        create_topic(topic_name="entity", num_partitions=n_entity)


    # read config
    config = ConfigParser()
    config.read(CONFIG_FILE_PATH)

    if gen_type == "classification":

        for entity in track(
            config.sections(), description="Generating classification data ..."
        ):
            logger.debug(entity)
            for _, n_samples in config.items(entity):
                logger.debug(n_samples)
                # to do
                # check n_jobs argument for multiprocessing
                generate_classification_data(
                    entity=entity,
                    n_samples=n_samples,
                    sink_type=sink_type,
                )

    if gen_type == "regression":
        for entity in track(
            config.sections(), description="Generating data regression data ..."
        ):
            logger.debug(entity)
            for _, n_samples in config.items(entity):
                generate_regression_data(
                    entity=entity,
                    n_samples=n_samples,
                    sink_type=sink_type,
                )


@app.command()
def cleanup(
    force: Annotated[
        bool, typer.Option(prompt=f"Are you sure you want to delete {TABLE_PATH} and {CONFIG_FILE_PATH}")
    ]
):
    """ Cleans up the data directory and config file. """
    if force:
        if TABLE_PATH.is_dir():
            import shutil
            shutil.rmtree(TABLE_PATH)
        if CONFIG_FILE_PATH.exists():
            Path.unlink(CONFIG_FILE_PATH)

    else:
        typer.echo("Operation cancelled")


if __name__ == "__main__":
    # uncomment below to debug in vscode
    # app(["generate-data","--sink-type","kafka"])
    app()
