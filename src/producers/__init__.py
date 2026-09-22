"""
Producers module for publishing data to Kafka topics.
"""

from src.producers.kafka_producer import get_kafka_producer, produce_ticks

__all__ = ["get_kafka_producer", "produce_ticks"]

