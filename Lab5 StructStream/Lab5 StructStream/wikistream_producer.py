# %% Dependencies
import socket
import json
from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams
from datetime import datetime, timedelta

# %% Helper Functions
# Serializer function to change message from python dict to json
value_serializer = lambda val: json.dumps(val).encode('utf-8')

# %% Producder Instantiation
# Set the producer configuration
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'compression.type': 'lz4'
}

# Instantiate the producer
producer = Producer(conf)

# %% Create wikistreams query
stream = EventStreams(
  streams=['recentchange', 'revision-create']
)
# Sans 'since', suivre les événements en direct plutôt qu'une ancienne date.
stream.register_filter(server_name='fr.wikipedia.org', type='edit')
# Pour personnaliser, remplacer la ligne ci-dessus par UNE des suivantes :
# stream.register_filter(server_name='en.wikipedia.org', type='edit')
# stream.register_filter(server_name='fr.wikipedia.org', type='edit', bot=False)
# stream.register_filter(server_name='fr.wikipedia.org', type='edit', title='Paris')

# %% Query EventStream
# Run a single query and inspect raw and example formatted output
change = next(stream)
print('Raw Message: ' + str(change))
print(
  '\n' # Add line breakd between outputs
  'Formatted Message: {type} on page "{title}" by "{user}" at {meta[dt]}.'
  .format(**change)
)

# %% Streaming Query
duration = 10 # Streaming window in minutes
start_time = datetime.now() # Current clock time
stop_time = start_time + timedelta(minutes=duration) #start+duration=stop

# Query stream for <duration> minutes
# Uncomment print change to examine output in interactive env
topic='wikistreams'

def delivery_report(error, message):
  if error is not None:
    raise RuntimeError(f'Échec de livraison Kafka : {error}')

try:
  # Envoyer aussi l'événement affiché dans la démonstration.
  producer.produce(topic=topic, value=value_serializer(change),
                   on_delivery=delivery_report)
  producer.poll(0)
  while datetime.now() < stop_time:
    change = next(stream)
    producer.produce(topic=topic, value=value_serializer(change),
                     on_delivery=delivery_report)
    producer.poll(0)  # Traiter les callbacks de livraison.
    print(' ')
    print(change)
except KeyboardInterrupt:
  print('\n arrêt demandé')
finally:
  # flush attend la livraison des messages encore dans la file.
  print('\n envoi des derniers messages puis arrêt du producer')
  remaining = producer.flush(timeout=30)
  if remaining:
    raise RuntimeError(f'{remaining} messages Kafka non livrés.')
