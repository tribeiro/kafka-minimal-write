#!/usr/bin/env python
import argparse
import time
import random
import json
import astropy.time
import os

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError

# Use broker:9092 if running within a Docker container, else "127.0.0.1:9092"

KAFKA_BROKER_ADDR = os.environ.get("LSST_KAFKA_BROKER_ADDR", "127.0.0.1:9092")

print(f"{KAFKA_BROKER_ADDR=}")

class StoreProducerStats:
    producer_stats = []

    def stats_callback(self, stats_json_str):
        stats = json.loads(stats_json_str)
        self.producer_stats.append(stats)
        # Process or print metrics as needed

def create_topics(
    topic_names: list[str],
    broker_addr: str,
    num_partitions=1,
    replication_factor=1,
) -> None:
    """Create missing Kafka topics.

    Parameters
    ----------
    topic_names : list[str]
        List of Kafka topic names
    broker_addr : str
        Kafka broker address.
    num_partitions : int
        Number of partitions for each topic.
    replication_factor : int
        Replication factor for each topic.
    """
    # Dict of kafka topic name: topic_info
    new_topics_info = [
        NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        for topic_name in topic_names
    ]
    username = os.environ.get("LSST_KAFKA_SECURITY_USERNAME", None)
    password = os.environ.get("LSST_KAFKA_SECURITY_PASSWORD", None)
    admin_client_config = {
            "bootstrap.servers": broker_addr,
            "security.protocol": "SASL_PLAINTEXT" if username is not None else None,
            "sasl.mechanism": "SCRAM-SHA-512" if username is not None else None,
            "sasl.username": username,
            "sasl.password": password,
    }
    if username is None:
        admin_client_config.pop("security.protocol")
        admin_client_config.pop("sasl.mechanism")
        admin_client_config.pop("sasl.username")
        admin_client_config.pop("sasl.password")
    
    broker_client = AdminClient(
            admin_client_config,
    )
    if new_topics_info:
        create_result = broker_client.create_topics(new_topics_info)
        for topic_name, future in create_result.items():
            exception = future.exception()
            if exception is None:
                print(f"created topic {topic_name!r}")
                continue
            elif (
                isinstance(exception.args[0], KafkaError)
                and exception.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS
            ):
                print(f"topic {topic_name!r} already exists")
            else:
                print(f"Failed to create topic {topic_name!r}: {exception!r}")
                raise exception


def main():
    parser = argparse.ArgumentParser("Write Kafka messages")
    parser.add_argument(
        "-n", "--number", type=int, default=1, help="Number of messages to produce_data"
    )
    parser.add_argument(
        "-i", "--index", type=int, default=1, help="Number of messages to produce_data"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="The number of partitions per topic.",
    )
    args = parser.parse_args()

    topic_name = f"lsst.sal.minimal.test.topic{args.index:04}"

    acks = 1

    create_topics(
        topic_names=[topic_name],
        broker_addr=KAFKA_BROKER_ADDR,
        num_partitions=args.partitions,
    )

    print(f"Create a producer with {acks=}")
    username = os.environ.get("LSST_KAFKA_SECURITY_USERNAME", None)
    password = os.environ.get("LSST_KAFKA_SECURITY_PASSWORD", None)

    store_producer_stats = StoreProducerStats()
    producer_configuration = {
            "acks": acks,
            "queue.buffering.max.ms": 0,
            "bootstrap.servers": KAFKA_BROKER_ADDR,
            "security.protocol": "SASL_PLAINTEXT" if username is not None else None,
            "sasl.mechanism": "SCRAM-SHA-512" if username is not None else None,
            "sasl.username": username,
            "sasl.password": password,
            "api.version.request": True,
            "statistics.interval.ms": 5000,
            "stats_cb": store_producer_stats.stats_callback,
    }

    if username is None:
        producer_configuration.pop("sasl.username")
        producer_configuration.pop("sasl.password")
        producer_configuration.pop("security.protocol")
        producer_configuration.pop("sasl.mechanism")

    producer = Producer(
        producer_configuration
    )
    producer.list_topics()
    print("before prod.")
    producer.flush()
    seq_num=0
    data_dict = dict(snd_timestamp=astropy.time.Time.now().to_string(), seq_num=seq_num, payload=f"{random.randbytes(1000)}")
    seq_num+=1
    raw_data = json.dumps(data_dict).encode()
    print(f"Produce 1 message")
    producer.produce(topic_name, raw_data)
    print("flush the first time.")
    t0 = time.time()
    producer.flush()
    t1 = time.time()
    dtflush = t1 - t0
    print(f"First flush took {dtflush:0.2f} seconds")
    print(f"Produce {args.number} messages")

    for i in range(args.number):
        data_dict = dict(snd_timestamp=astropy.time.Time.now().to_string(), seq_num=seq_num, payload=f"{random.randbytes(1000)}")
        seq_num+=1
        raw_data = json.dumps(data_dict).encode()
        t0 = time.monotonic()

        def callback(err, _) -> None:
            if err:
                print(f"Error producing message: {err}")
            else:
                dt = time.monotonic() - t0
                if dt > 0.1:
                    print(
                        f"warning: {topic_name} "
                        f"took {dt:0.2f} seconds."
                    )
        producer.produce(
            topic_name,
            raw_data,
            on_delivery=callback,
        )
        producer.flush()
    dt = time.time() - t1
    print(f"Wrote {args.number/dt:0.1f} messages/second, {len(raw_data)/1000/dt:0.1f} k-bytes/s: {args} (after the first message)")
    producer_stats_filename = f"producer_stats_{args.index:04}.json"
    print(f"Storing producer statistics in {producer_stats_filename}.")
    with open(producer_stats_filename, "w") as fp:
        fp.write(json.dumps(store_producer_stats.producer_stats))

if __name__ == "__main__":
    main()
