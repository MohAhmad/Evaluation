import time
import paho.mqtt.client as mqttClient
import csv

 
def on_connect(client, userdata, flags, rc):
 
    if rc == 0:
 
        print("Connected to broker")
 
        global Connected                #Use global variable
        Connected = True                #Signal connection 
 
    else:
 
        print("Connection failed")
 
Connected = False   #global variable for the state of the connection
 
broker_address= "172.17.0.2"
port = 1883
user = "guest"
#password = "guest"
 
client = mqttClient.Client("publisher")              #create new instance
#client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.connect(broker_address, port=port)          #connect to broker
 
client.loop_start()        #start the loop
 
while Connected != True:    #Wait for connection
    time.sleep(0.1)
    

        
try:
    while True:
        with open("AirQualityUCI.csv", "r") as f:
    	    reader = csv.reader(f)
    	    for row in reader:
                row = ', '.join(row)
                client.publish("airQuality",row)
                time.sleep(0.01)
 
except KeyboardInterrupt:
 
    client.disconnect()
    client.loop_stop()
