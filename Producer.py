import asyncio
import json
import time
import os
import paho.mqtt.client as mqtt
from azure.iot.device.aio import IoTHubModuleClient

# MQTT settings
MQTT_BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', '1883'))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'producer/telemetry')

# Azure IoT settings
iot_hub_module_client = None

# Create the IoT Hub Module Client
async def init_iot_client():
    global iot_hub_module_client
    iot_hub_module_client = IoTHubModuleClient.create_from_edge_environment()
    await iot_hub_module_client.connect()
    print("IoT Hub module client initialized.")

# Publish a message to the MQTT broker
def publish_message(mqtt_client):
    message = {
        "source": "producer",
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "data": {
            "value": 42
        }
    }
    payload = json.dumps(message)
    mqtt_client.publish(MQTT_TOPIC, payload)
    print(f"Published message: {payload}")

# Initialize the MQTT client and connect to the broker
def init_mqtt_client():
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    return mqtt_client

# MQTT connect callback
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    publish_message(client)

# MQTT disconnect callback
def on_disconnect(client, userdata, rc):
    print(f"Disconnected from MQTT broker with result code {rc}")

# Main function
async def main():
    await init_iot_client()
    mqtt_client = init_mqtt_client()
    mqtt_client.loop_start()

    # Keep publishing messages at intervals
    try:
        while True:
            publish_message(mqtt_client)
            await asyncio.sleep(10)  # Publish every 10 seconds
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        await iot_hub_module_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
