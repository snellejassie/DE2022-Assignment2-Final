from kafka import KafkaProducer


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.140.66.33:9092')  # use your VM's external IP Here!
    with open('C:\Users\20181980\Documents\DSBE_Y1S1\Data Engineering\Assignment 2 for hand in\Output_Files\datawordcount.txt') as f:
        lines = f.readlines()

    for line in lines:
        kafka_python_producer_sync(producer, line, 'word')

    f.close()
