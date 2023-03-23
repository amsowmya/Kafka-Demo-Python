from confluent_kafka import Consumer

consumer = Consumer({'bootstrap.servers':'localhost:9092', 'group.id':'python-consumer', 'auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

print('Available topics to consume: ', consumer.list_topics().topics)
consumer.subscribe(['user-tracker'])
print('Subscribed to the topic : user-tracker')

def main():
    while True:
        msg = consumer.poll(2.0)
        if msg is None:
            print("Msg is none")
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        data = msg.value().decode('utf-8')
        print(data)
    consumer.close()

if __name__ == '__main__':
    main()