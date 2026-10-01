# %%
import socket
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'message.timeout.ms': 30000}

producer = Producer(conf)
topic = 'book_topic'
delivery_errors = 0


def delivery_report(error, msg):
    global delivery_errors
    if error is not None:
        delivery_errors += 1
        print(f'Delivery failed: {error}')


# %%
try:
    with open('book.txt', 'r', encoding='utf-8') as book:
        for line in book:
            # Send blank lines too: every source line is a message.
            producer.produce(topic=topic, value=line.encode('utf-8'),
                             callback=delivery_report)
            print(f'Sending: {line.rstrip()}')
            producer.poll(0)
except KeyboardInterrupt:
    print('Producer interrupted; flushing queued messages.')
finally:
    producer.flush()

if delivery_errors:
    raise SystemExit(f'{delivery_errors} messages could not be delivered.')
