# python3.6

import random
import re
import pandas as pd
from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
topic = "vitals/mqtt"
# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'

trackNames = pd.read_csv('track_names.csv')
tracksKeys = {}
for i in range(len(trackNames)):
    # create json where keys are devices/tracks index and values are min, max ranges
    tracksKeys[str(i)] = str(trackNames.loc[i, "Min"]) + "," + str(trackNames.loc[i, "Max"])

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        manageData(msg.payload.decode())

    client.subscribe(topic)
    client.on_message = on_message

def manageData(data):
    patients = data.split(":")
    for i in range(len(patients)):
        filename = "patient " + str(i+1)
        filedata = ""
        print(filename)
        if ";" in patients[i]:
            patient = patients[i]
            d = patient.split(",")
            filedataarr = []
            for j in range(len(d)):
                dt = d[j]
                index = re.findall(r'\d+', dt.split(";")[0])
                value = dt.split(";")[1]
                print(index[0])
                print("original value", value)
                ranges = tracksKeys[index[0]].split(",")
                min = int(ranges[0])
                max = int(ranges[1])
                floatValue = float(value)
                updatedValue = 0
                if(floatValue >= min and floatValue <= max):
                    updatedValue = 1
                else:
                    updatedValue = floatValue
                
                print("updated value", updatedValue)
                filedataarr.append(str(index[0]) + ";" + str(updatedValue))
            filedata = ",".join(filedataarr)
        
        text_file = open(filename+".txt", "a")
        text_file.write(filedata)
        text_file.close()
    print("---------------------")


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


run()
