#!/bin/python

# kafka-run-class.sh kafka.tools.SimpleConsumerShell --broker-list <broker:port,...> --topic <topic_name> --partition <partition_id> --print-offsets --offset <offset> --max-messages <num_max_msgs>

from argparse import ArgumentParser
from datetime import datetime
from kafka import KafkaConsumer
import sys

from MyPartition import MyPartition
from Colors import Colors

if __name__ == '__main__':

    parser = ArgumentParser(description='Find the offset of a message received a given timestamp with a given accuracy in each partition of a given Apache Kafka topic.')
    parser.add_argument('-a','--accuracy', choices=['day', 'hour', 'minute', 'second'], default='day', help='accuracy level of the offset research (day by default)')
    parser.add_argument('brokers', help='comma separated list of the brokers, each broker must be in the form host:port')
    parser.add_argument('topic', help='name of the topic')
    parser.add_argument('timestamp', help='timestamp in the form yyyy-mm-ddTHH:MM:SS')

    namespace = parser.parse_args(sys.argv[1:])
    args = vars(namespace)

    timestamp = datetime.strptime(args['timestamp'], '%Y-%m-%dT%H:%M:%S')
    accuracy, servers, topic_name = args['accuracy'], args['brokers'].split(','), args['topic']

    # consumer = KafkaConsumer(bootstrap_servers=['dev-slave01.hdp-betsson.com:6667'],consumer_timeout_ms=500)
    consumer = KafkaConsumer(bootstrap_servers=servers, consumer_timeout_ms=20000)
    partitionIds = consumer.partitions_for_topic(topic_name)  # get partitions ids

    print Colors.HEADER + '# of partitions: ' + str(len(partitionIds)) + Colors.ENDC

    for id in partitionIds:
        my_partition = MyPartition(accuracy, servers, topic_name, id, timestamp)
        offset = my_partition.get_offset()
        print Colors.OKBLUE + '--> offset in partition ' + str(id) + ': ' + str(offset) + Colors.ENDC

    sys.exit(0)
