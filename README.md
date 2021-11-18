Instructions to start the evaluation environment.

1- Start the MQTT Broker:
    sudo docker run -it -p 1883:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf

2- Start the MQTT Publisher:
    sudo python3 MQTTPublisher.py

3- Start the containernet emulation:
    sudo python3 Infrastructure.py --depth 8 --fanout 2 --sleep 10

