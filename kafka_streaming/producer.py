from confluent_kafka import Producer, KafkaError
import json
import utils.ccloud_lib as ccloud_lib

def create_message(value):
     record_key = "alice"
     record_value = json.dumps({'count': value})
     return { "record_key": record_key, "record_value": record_value}

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

if __name__ == '__main__':
    args = ccloud_lib.parse_args()
    producer_conf = ccloud_lib.read_ccloud_config(args.config_file)
    
    # Create topic if needed
    ccloud_lib.create_topic(producer_conf, args.topic)
    producer = Producer(producer_conf)

    for i in range(10):
        msg = create_message(i)
        producer.produce(args.topic, key=msg["record_key"], value=msg["record_value"], on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
    producer.flush()