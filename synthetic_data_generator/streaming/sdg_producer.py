from confluent_kafka import Producer
from synthetic_data_generator.streaming.kafka_config import producer_config
from confluent_kafka.admin import AdminClient, NewTopic
import logging
from rich.logging import RichHandler
import numpy as np 
import pandas as pd 
import json

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")

def callback(err, event):
    if err:
        logger.error(f"Produce to topic {event.topic()} failed for event: {event.key()}")
    else:
        val = event.value().decode("utf8")
        logger.info(f"{val} sent to partition {event.partition()}.")

def create_topic(topic_name:str, num_partitions: int)->None:
    """ Create topic

    :param topic_name: topic name
    :param num_partitions: number of partitions
    """
    ac = AdminClient(producer_config)
    topic = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=1)]
    res = ac.create_topics(topic)

    # Wait for each operation to finish.
    for topic, f in res.items():
        try:
            f.result()  # The result itself is None
            logger.info("Topic {} created".format(topic))
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")


def create_chunks(df: pd.DataFrame,chunk_size:int=1000)-> list:
        
        num_of_rows = df.shape[0]
        logger.debug(f'Number of rows: {num_of_rows}')
        chunks = []
        num_chunks = int(np.ceil(num_of_rows / chunk_size))
        for i in range(num_chunks):
            start = chunk_size * i
            stop = start + chunk_size
            chunks.append(df[start:stop])
        return chunks

def send_to_kafka(topic_name: str, df: pd.DataFrame, key:str):
        if df.empty:
             raise ValueError("Dataframe is empty, nothing to be sent.")
        
        chunks = create_chunks(df=df)

        producer = Producer(producer_config)

        # iterate over chunks    
        for i in range(len(chunks)):
            logger.debug(f'Chunk {i}: {len(chunks[i])}')

            chunk_dict = chunks[i].to_dict()

            # Encode the dictionary into a JSON Byte Array
            data = json.dumps(chunk_dict, default=str).encode('utf-8')
            # Send the data to kafka
            logger.debug(f"**************** Data for chunk {i}********************** ")
            producer.produce(topic=topic_name, key=key, value=data)
            producer.flush() 


if __name__ == "__main__":
    ac = AdminClient(conf=producer_config)
    logger.info(ac.list_topics().topics)

