# %%
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(config)

# %%
topic = 'book_topic'
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)],
    request_timeout=10
)

try:
    futures[topic].result()
    print(f'Created topic: {topic}')
except KafkaException as error:
    if error.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
        print(f'Topic already exists: {topic}')
    else:
        raise

# %%
x = admin_client.list_topics(timeout=10)
for t in x.topics.keys():
    print(t)
