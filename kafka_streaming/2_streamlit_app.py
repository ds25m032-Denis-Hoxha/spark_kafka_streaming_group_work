import streamlit as st
import cv2
import numpy as np
import imutils
from confluent_kafka import Producer

def get_kafka_producer():
    return Producer({
        'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'TGYVM6H5NCVC5EII',
        'sasl.password': 'ySPBA/RJVdw+k0PgKsgN04Z0VqcqDGKi6RfDqx7Rok/4703E7AX/GjBU12zXs7mP'
    })

# Function to convert image to bytes suitable for Kafka
def convert_image_to_bytes(image):
    # Reduce the resolution if the high resolution is not necessary
    image = cv2.resize(image, (360, 240), interpolation=cv2.INTER_LINEAR)
    # Compress the image to reduce size and speed up encoding
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 70]
    _, buffer = cv2.imencode('.jpg', image, encode_param)
    return buffer.tobytes()

def main():
    st.title("Capture and Send Images to Kafka")
    topic_name = st.text_input("Kafka Topic", "webcam-feed")

    producer = get_kafka_producer()
    img_file_buffer = st.camera_input("Capture from your webcam")

    if img_file_buffer is not None:
        file_bytes = np.asarray(bytearray(img_file_buffer.getvalue()), dtype=np.uint8)
        img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)
        st.image(img, caption='Captured Image', use_column_width=True)

        img_bytes = convert_image_to_bytes(img)
        producer.produce(topic_name, img_bytes)
        producer.poll(0)  # Non-blocking, handle delivery reports on callback.
        st.success("Frame sent to Kafka")

    st.button("Flush Kafka", on_click=lambda: producer.flush())

if __name__ == '__main__':
    main()