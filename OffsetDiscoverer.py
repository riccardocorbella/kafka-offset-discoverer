#!/bin/python

# kafka-run-class.sh kafka.tools.SimpleConsumerShell --broker-list <broker:port,...> --topic <topic_name> --partition <partition_id> --print-offsets --offset <offset> --max-messages <num_max_msgs>

from kafka import KafkaConsumer
import sys

from MyPartition import MyPartition

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

if __name__ == '__main__':

    if len(sys.argv) < 6:
        print bcolors.FAIL + 'Usage: <broker:port,broker:port,...> <topic name> <year> <month> <day>' + bcolors.ENDC
        sys.exit(1)

    servers = sys.argv[1].split(',')
    topic_name = sys.argv[2]
    year = int(sys.argv[3])
    month = int(sys.argv[4])
    day = int(sys.argv[5])

    # consumer = KafkaConsumer(bootstrap_servers=['dev-slave01.hdp-betsson.com:6667'],consumer_timeout_ms=500)
    consumer = KafkaConsumer(bootstrap_servers=servers, consumer_timeout_ms=20000)
    partitionIds = consumer.partitions_for_topic(topic_name)  # get partitions ids

    print bcolors.HEADER + '# of partitions: ' + str(len(partitionIds)) + bcolors.ENDC

    for id in partitionIds:
        my_partition = MyPartition(servers, topic_name, id, year, month, day)
        offset = my_partition.get_offset()
        print bcolors.OKGREEN + '--> offset in partition ' + str(id) + ': ' + str(offset) + bcolors.ENDC

    sys.exit(0)
