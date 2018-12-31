import logging
import argparse
from kafka import KafkaConsumer, TopicPartition

# logger
_logger = logging.getLogger('offset_change')
streamHandler = logging.StreamHandler()
_logger.addHandler(streamHandler)
_logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    _logger.info("=== start change consumer offset")

    parser = argparse.ArgumentParser(description='Do you need to change for consumer group offset in kafka?')
    parser.add_argument('--file', type=str, help='What is the file with the partitions written on?', required=True)
    parser.add_argument('--topic', type=str, help='What is the topic?', required=True)
    parser.add_argument('--group', type=str, help='What is the group?', required=True)
    args = parser.parse_args()

    file = args.file
    topic = args.topic
    group = args.group

    # get each offset of partition
    partitions = {}
    with open(file, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            p = line.replace("\r", "").replace("\n", "").split(',')

            if not p[0].isdigit():
                _logger.error('partition is not number')
                exit()
            if not p[1].isdigit():
                _logger.error('offset is not number')
                exit()
            partitions[int(p[0])] = int(p[1])

    # kafka consumer
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
        group_id=group
    )

    for k in partitions.keys():
        _logger.info('partitions:{}, offset:{}'.format(k, partitions[k]))

        tp = TopicPartition(topic, k)
        consumer.assign([tp])
        consumer.seek(tp, partitions[k])
        consumer.commit()

    consumer.close()
