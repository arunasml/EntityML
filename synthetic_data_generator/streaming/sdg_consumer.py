from confluent_kafka import Consumer, KafkaError, KafkaException
from synthetic_data_generator.streaming.kafka_config import consumer_config
from rich.logging import RichHandler
import sys
import pandas as pd
import json
import logging


FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")

running = True


def process_msg(msg: any):
    df = pd.DataFrame(json.loads(msg.value().decode("utf8")))
    print(df.head())


def basic_consume_loop(topics):
    consumer = Consumer(consumer_config)
    try:
        consumer.subscribe(topics)

        while running:

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.info("RUNNING")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_msg(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    basic_consume_loop(topics=["entity"])
