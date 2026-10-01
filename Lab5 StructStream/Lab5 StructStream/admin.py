# %%
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka est accessible depuis la machine hôte.
config = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(config)
topic = 'wikistreams'

# Attendre le résultat de la création avant de vérifier le topic.
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)],
    request_timeout=30,
)
try:
    futures[topic].result()
    print(f'Topic {topic} créé.')
except KafkaException as exc:
    if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
        raise
    print(f'Topic {topic} déjà présent.')

metadata = admin_client.list_topics(timeout=30)
if topic not in metadata.topics or metadata.topics[topic].error is not None:
    raise RuntimeError(f'Topic {topic} indisponible.')
for name in metadata.topics:
    print(name)

# admin_client.delete_topics([topic])
