import logging
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer

# logger
_logger = logging.getLogger('dummy_logger')
streamHandler = logging.StreamHandler()
_logger.addHandler(streamHandler)
_logger.setLevel(logging.DEBUG)

# scheduler
schedule = BlockingScheduler()

# topic
_topic = "test-topic"
_producer = KafkaProducer(bootstrap_servers='localhost:9092')


@schedule.scheduled_job('interval', seconds=5, id="dummy_kafka")
def scheduler():
    now = datetime.datetime.now()
    _logger.info("[run] scheduler - {}".format(now))
    _producer.send(_topic, b'test value - {}'.format(now))


if __name__ == '__main__':
    _logger.info("=== start dummy into kafka ===")

    schedule.start()
