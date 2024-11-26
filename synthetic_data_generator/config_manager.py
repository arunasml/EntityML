from pydantic_settings import BaseSettings
from pydantic import BaseModel
from pathlib import Path

import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class SplitSetting(BaseModel):
    high: int
    low: int


class SampleRangeSetting(BaseModel):
    start: int
    end: int


class ConfSettings(BaseSettings):

    split_setting: SplitSetting = SplitSetting(high=80, low=20)
    table_path: Path = Path(r"./data/delta")
    high_threshold_sample_setting: SampleRangeSetting = SampleRangeSetting(
        start=100, end=1000
    )
    low_threshold_sample_setting: SampleRangeSetting = SampleRangeSetting(
        start=1000, end=10000
    )

    # config file to store entity and num of rows mapping
    config_file_path: Path = Path(r"./conf/config.ini")
    n_feature: int = 20

    class Config:
        env_prefix = "cluster_"


if __name__ == "__main__":
    logger.info(ConfSettings())
