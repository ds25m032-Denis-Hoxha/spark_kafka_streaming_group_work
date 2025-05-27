
import faust
from faust.types.auth import AuthProtocol
import ssl
from utils import ccloud_lib
from faust_music_events import MusicEvent

# Read the Kafka configuration
kafka_app_config = ccloud_lib.read_ccloud_config("kafka.config")

# Set up SASL credentials
creds = faust.SASLCredentials(
    username=kafka_app_config['sasl.username'],
    password=kafka_app_config['sasl.password'],
    mechanism='PLAIN',
    ssl_context=ssl.create_default_context()
)

# Initialize the Faust app
app = faust.App('music_stream_processor',
                topic_replication_factor=3,
                topic_partitions=1,
                broker=f"kafka://{kafka_app_config['bootstrap.servers']}",
                value_serializer='json',
                store='rocksdb://',
                broker_credentials=creds)

# Define a Kafka topic with MusicEvent as the value type
topic = app.topic('music_streams', value_type=MusicEvent)
song_plays = app.Table('song_plays', default=int)

# Define a stream processor
@app.agent(topic)
async def process(stream):
    async for event in stream:
        song_plays[event.userId] += 1
        print(f'User {event.userId} has listened to {song_plays[event.userId]} songs.')


@app.page('/counts/{userId}/')
async def get_count(self, request, userId):
    count = song_plays[userId]
    return app.web.json({'userId': userId, 'count': count})

@app.page('/counts/')
async def get_all_counts(self, request):
    # Create a dictionary to store all user counts
    all_counts = [{'userId': user_id, 'songplays': count} for user_id, count in song_plays.items()]
    return app.web.json(all_counts)
