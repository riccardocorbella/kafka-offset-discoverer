from kafka import KafkaConsumer
from kafka import TopicPartition
from datetime import datetime
import json

class MyPartition:
    def __init__(self, accuracy, servers, topic_name, partition_id, timestamp):
        self.accuracy = accuracy
        self.consumer = KafkaConsumer(bootstrap_servers=servers, consumer_timeout_ms=20000)
        self.partition = TopicPartition(topic_name, partition_id)
        self.consumer.assign([self.partition])
        self.referringDate = timestamp

    def search(self, l, r, solutions):
        if l > r:
            if len(solutions) > 0:
                return list(solutions)[0]
            else:
                None

        m = (l + r) / 2
        event = self.read_event(m)

        date = None
        if self.accuracy == "hour":
            date = datetime.strptime(event['Timestamp'][:13], '%Y-%m-%dT%H') #':%M:%S')
        elif self.accuracy == "minute":
            date = datetime.strptime(event['Timestamp'][:16], '%Y-%m-%dT%H:%M')
        elif self.accuracy == "second":
            date = datetime.strptime(event['Timestamp'][:19], '%Y-%m-%dT%H:%M:%S')
        else:
            date = datetime.strptime(event['Timestamp'][:10], '%Y-%m-%d')

        if date < self.referringDate:
            l = m + 1
            return self.search(l, r, solutions)
        elif date > self.referringDate:
            r = m - 1
            return self.search(l, r, solutions)
        else:
            r = m - 1
            solutions.add(m)
            return self.search(l, r, solutions)

    def read_event(self, offset):
        self.consumer.seek(self.partition, offset)
        return json.loads(next(self.consumer).value)

    def get_offset(self):
        self.consumer.seek_to_beginning(self.partition)
        l = self.consumer.position(self.partition) - 1
        self.consumer.seek_to_end(self.partition)
        r = self.consumer.position(self.partition) - 1
        return self.search(l, r, set())
