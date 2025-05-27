from utils.kafka_connector import run_consumer
from confluent_kafka import Producer, KafkaError

import utils.ccloud_lib as ccloud_lib

def process_data(data):
    print(data)

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    conf = ccloud_lib.read_ccloud_config(args.config_file)

    # Create Consumer instance
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    run_consumer(conf, 'python_example_group', args.topic, process_data)