import logging
import threading

import kafka_producer.kafka_client as kafka_client
import kafka_producer.kafka_config as kafka_config
import rules.stream_rules as stream_rules
import streamer_with_retry
from init_logger import init_logger

init_logger()
logger = logging.getLogger(__name__)

# Prepare Kafka client
# TODO: Pending to create new client on kafka error
logger.info("Kafka producer init started")
kafka_producer = kafka_client.KafkaProducer(
    kafka_config.server_endpoint,
    topic="twitter-job-tweets-2")
kafka_producer.init_producer()
logger.info("Kafka producer init completed")


# callback to call on arrival of new tweet
def on_data(data):
    if isinstance(data, str):
        kafka_producer.produce("tweet", data)
    print(data)
    print("=================================================")
    print("")


# Check filter rules and update them if required
stream_rules.commit_filter_rules_if_required()

# Start streamer on new thread
thrd = threading.Thread(target=streamer_with_retry.start, args=(on_data, ))
thrd.start()
logger.info("streamer thread started")

choice = ''
while choice != 'q' and choice != 'Q':
    choice = input("Press q/Q to exit")

logger.info("Terminating the application")
streamer_with_retry.stop()
thrd.join()
logger.info("Streamer stopped")
kafka_producer.complete()
logger.info("Termination completed")
