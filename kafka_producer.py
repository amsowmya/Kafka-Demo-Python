from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random

fake = Faker()

logging.basicConfig(format = '%(asctime)s %(message)s',
                    datefmt = '%Y-%m-%d %H:%M:%S',
                    filename = 'producer.log',
                    filemode = 'w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#################
producer = Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')
####################

def receipt(err, msg):
    if err is not None:
        print(f'Error: {err}')
    else:
        message = f"Produced message on topic {msg.topic()}, with value of {msg.value().decode('utf-8')} \n"
        logger.info(message)
        print(message)

def main():
    for i in range(10):
        data = {
            'user_id': fake.random_int(min=20000, max=100000),
            'user_name': fake.name(),
            'user_address': fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
            'platform': random.choice(['Mobile', 'Laptop', 'Tablet']),
            'signup_at': str(fake.date_time_this_month())
        }
        m = json.dumps(data)
        producer.poll(1)
        producer.produce('user-tracker', m.encode('utf-8'), callback=receipt)
        producer.flush()
        time.sleep(3)

if __name__ == '__main__':
    main()