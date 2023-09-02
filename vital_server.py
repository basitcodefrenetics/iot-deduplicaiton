import random
import time
import vitaldb
import pandas as pd

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
topic = "vitals/mqtt"
# Generate a Client ID with the publish prefix.
client_id = f'publish-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'

# assign delimeters
indexValueDelimiter = ";"
valueDelimiter = ","
patientDelimeter = ":"

#import track names
trackNames = pd.read_csv('track_names.csv')

def connect_mqtt():
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


def publish(client, msg):
    time.sleep(1)
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")

def manageData(data, dataTrackName):
    concatIndexValue = ""
    # get valid indexes of values and values from data
    validIndexes = [i for i in range(len(data)) if pd.isnull(data[i]) is False]
    validValues = [data[i] for i in range(len(data)) if pd.isnull(data[i]) is False]
    # pick track names of the valid values from the data
    validDataTrackName = [dataTrackName[index] for index in validIndexes]
    # get index of the valid Data Track Names from the track_names dataset
    trackNamesIndex = [trackNames.index[trackNames['Parameter'] == validDataTrackName[i]][0] for i in range(len(validDataTrackName)) if len(trackNames.index[trackNames['Parameter'] == validDataTrackName[i]]) > 0]

    if len(trackNamesIndex) == len(validValues):
        # concat column id with value in each element in both list
        concatIndexValueList = [str(i) + indexValueDelimiter + str(j) for i,j in zip(trackNamesIndex, validValues)]
        concatIndexValue = valueDelimiter.join(concatIndexValueList)
    
    return concatIndexValue

def run():

    #initialize data
    print("loading vitals files")
    vf1 = vitaldb.VitalFile('0001.vital')
    vf2 = vitaldb.VitalFile('0002.vital')
    vf3 = vitaldb.VitalFile('0003.vital')
    vf4 = vitaldb.VitalFile('0004.vital')
    vf5 = vitaldb.VitalFile('0005.vital')
    print("vitals files loaded")

    # convert to numpy
    print("initializing vitals files to list")
    vf1Data = vf1.to_numpy(vf1.get_track_names(), interval = 1)
    vf2Data = vf2.to_numpy(vf2.get_track_names(), interval = 1)
    vf3Data = vf2.to_numpy(vf3.get_track_names(), interval = 1)
    vf4Data = vf2.to_numpy(vf4.get_track_names(), interval = 1)
    vf5Data = vf2.to_numpy(vf5.get_track_names(), interval = 1)
    print("initializing completed")

    # connect mqtt
    client = connect_mqtt()
    client.loop_start()

    for v in range(10000, 11000):
        v1Conctat = ""
        v2Conctat = ""
        v3Conctat = ""
        v4Conctat = ""
        v5Conctat = ""

        if(len(vf1Data[v]) > 0):
            v1Conctat = manageData(vf1Data[v], vf1.get_track_names())
        if(len(vf2Data[v]) > 0):
            v2Conctat = manageData(vf2Data[v], vf2.get_track_names())
        if(len(vf3Data[v]) > 0):
            v3Conctat = manageData(vf3Data[v], vf3.get_track_names())
        if(len(vf4Data[v]) > 0):
            v4Conctat = manageData(vf4Data[v], vf4.get_track_names())
        if(len(vf5Data[v]) > 0):
            v5Conctat = manageData(vf5Data[v], vf5.get_track_names())
        
        data = v1Conctat + patientDelimeter + v2Conctat + patientDelimeter + v3Conctat + patientDelimeter + v4Conctat + patientDelimeter + v5Conctat

        publish(client, data)
        print("range: " + str(v))
        print("---------------------------")

    client.loop_stop()


run()