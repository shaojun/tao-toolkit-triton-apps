import datetime
import os
import paho.mqtt.client as mqtt
import json
import base64
from io import BytesIO
from PIL import Image

# MQTT settings
MQTT_BROKER = "mqtt0.glfiot.com"
MQTT_PORT = 1883
MQTT_TOPIC = "board/E1675418534340988929/elenet/outbox"

# Callback when the client receives a message


def on_message(client, userdata, msg):
    datetime_str = datetime.datetime.now().strftime("%H_%M_%S")
    try:
        # Parse the JSON message
        message = json.loads(msg.payload.decode())
        data = message['event']['data']

        # Iterate over the base64-encoded images
        for i, img_base64 in enumerate(data):
            # Decode the base64 string
            img_data = base64.b64decode(img_base64)
            img = Image.open(BytesIO(img_data))

            # Save the image to a file
            img_filename = f"{datetime_str}_image_{i+1}.jpg"
            img_full_path = os.path.join(
                os.getcwd(), img_filename)
            img.save(img_full_path)
            print(f"Saved {img_full_path}")
    except Exception as e:
        print(f"Failed to process message: {e}")


# Create an MQTT client and attach the callback
client = mqtt.Client()
client.on_message = on_message

# Connect to the MQTT broker and subscribe to the topic
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC)

# Start the MQTT client loop
client.loop_forever()
