import socket

producer_config = {
    "bootstrap.servers": "127.0.0.1:9092",
    "client.id": socket.gethostname(),
}
consumer_config = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "sdg-consumer",
    "enable.auto.commit": "true",
    "auto.offset.reset": "earliest",
}
