# %%
import re
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book_group',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
topic = 'book_topic'

# %%
MAX_EMPTY_POLLS = 30  # Ends after about 30 seconds of silence.
MAX_ERRORS = 5
empty_polls = 0
error_count = 0

try:
    consumer.subscribe([topic])
    # Append so restarting the same consumer group preserves previous output.
    with open('cleaned_book.txt', 'a', encoding='utf-8') as output:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1
                error_count = 0
                if empty_polls >= MAX_EMPTY_POLLS:
                    print('Closing: No new messages received.')
                    break
                continue

            if msg.error():
                empty_polls = 0
                error_count += 1
                print(f'Consumer error: {msg.error()}')
                if error_count >= MAX_ERRORS:
                    raise RuntimeError('Closing: Too many consecutive errors.')
                continue

            empty_polls = 0
            error_count = 0
            line = msg.value().decode('utf-8')
            line = line.lower()
            # Keep English letters and whitespace; separate punctuation with spaces.
            line = re.sub(r'[^a-z\s]', ' ', line)
            line = re.sub(r'\s+', ' ', line).strip()

            if line:
                print(line)
                output.write(line + '\n')
                output.flush()
except KeyboardInterrupt:
    print('Consumer stopped by user.')
finally:
    consumer.close()
