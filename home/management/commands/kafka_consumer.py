from django.core.management.base import BaseCommand
from confluent_kafka import Consumer,KafkaException,KafkaError
# from typing import Any


import json
from home.models import LocationUpdate
import os



class Command(BaseCommand):
    help = "Run kafka Consumer to listen for location update"
    # def handle(self, *args:json.Any, **options:json.Any) -> str | None:

    def handle(self, *args, **options):
        conf = {
            'bootstrap.servers':'localhost:9092',
            'group.id' : 'location_group',
            "auto.offset.reset" : 'earliest',
        }
        consumer = Consumer(conf)
        consumer.subscribe(['location_updates'])
        try:
            while True:
                msg = consumer.poll(timeout = 1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                data = json.loads(msg.value().decode('utf-8'))    
                LocationUpdate.objects.create(
                    latitude = data['latitude'],
                    longitude = data['longitude']
                )
                print(f"Received and saved {data}")
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()        

