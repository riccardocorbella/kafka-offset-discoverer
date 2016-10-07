# kafka-offset-discoverer

OffsetDiscover.py discovers with a given accuracy (day, hour, second) the offset of one of the first messages (must be a JSON object) that has been received in a given point in time in each partition of a given Apache Kafka topic.

### Requirements

* Kafka-python (https://github.com/dpkp/kafka-python)
