#!/usr/bin/env python3

import paho.mqtt.client as paho
import time
import json
import sys
import argparse
import logging
import datetime
from threading import Thread
import threading
import os
import glob
import io
#import pdb
try:
    import picamera
    import astral
except:
    pass
from base64 import b64encode

os.system('modprobe w1-gpio')
os.system('modprobe w1-therm')

base_dir = '/sys/bus/w1/devices/'

class ThermometerProtocol(object):

    def new_reading(self, host, sensor, reading, timestamp=None):

        if timestamp is None:
            timestamp = str(datetime.datetime.now())

        data = {'reading':{'host':host, 'sensor':sensor, 'reading':reading, 'timestamp':timestamp}}
        return json.dumps(data)

    def new_image(self, host, image, timestamp=None):
        if timestamp is None:
            timestamp = datetime.datetime.now()
        timestamp = str(timestamp)

        data = {'surveillance':{'host':host, 'image':image, 'timestamp':timestamp}}
        return json.dumps(data)

    def new_notification(self, notification):
        data = {'notification':notification}
        return json.dumps(data)

    def new_sensor(self, host, sensor):
        data = {'register_sensor':{'host':host, 'sensor':sensor}}
        return json.dumps(data)

class CameraReader(Thread):

    def __init__(self, client, topic, identifier, interval=120, resolution=(2592, 1944)):
        super(CameraReader, self).__init__()
        self.interval = interval
        self.client = client
        self.topic = topic
        self.identifier = identifier
        self.resolution = resolution
        self.suncache = None

    def sundata(self):
        now = datetime.datetime.now()
        if(self.suncache is not None):
            if (self.suncache.day >= now.day):
                return

        self.suncache = now
        a = astral.Astral()
        a.solar_depression = 'civil'
        sun = a['Copenhagen'].sun(now,local=True)
        self.dusk = sun['dusk']
        self.dawn = sun['dawn']
        self.sunrise = sun['sunrise']
        self.sunset = sun['sunset']

    def run(self):
        protocol = ThermometerProtocol()
        while True:
            try:
                #pdb.set_trace()
                camera = picamera.PiCamera()
                camera.resolution = self.resolution
                self.sundata()
                now = datetime.datetime.now()
                if(now.hour >= self.dawn.hour and now.hour <= self.dusk.hour):
                    logging.info("capturing image")
                    with io.BytesIO() as stream:
                        camera.capture(stream,'jpeg')
                        stream.seek(0)
                        b64data = b64encode(stream.read())
                        b64data = b64data.decode('ascii')
                        data = protocol.new_image(self.identifier,b64data)
                        logging.debug(data[:100])
                        self.client.publish(self.topic,data,0)
            except Exception as e:
                logging.exception(e)
            time.sleep(self.interval)


class ThermometerReader(Thread):
    def read_temp_raw(self,device):
        f = open(device, 'r')
        lines = f.readlines()
        f.close()
        return lines

    def read_temp(self, device):
        lines = self.read_temp_raw(device)
        while lines[0].strip()[-3:] != 'YES':
            time.sleep(0.2)
            lines = self.read_temp_raw()
        equals_pos = lines[1].find('t=')
        if equals_pos != -1:
            temp_string = lines[1][equals_pos+2:]
            temp_c = float(temp_string) / 1000.0
            temp_f = temp_c * 9.0 / 5.0 + 32.0
            return temp_c, temp_f

    def __init__(self, hostid, sensorId, device, interval=60, mqttClient=None, mqttTopic=None):
        super(ThermometerReader, self).__init__()
        self.device = device
        self.interval = interval
        self.mqttClient = mqttClient
        self.mqttTopic = mqttTopic
        self.sensorId = sensorId
        self.hostId = hostid
        self.daemon = True

    def run(self):
        protocol = ThermometerProtocol()
        while True:
            temp = self.read_temp(self.device)[0]
            logging.info("read %f from %s" % (temp,self.sensorId))
            data = protocol.new_reading(self.hostId,self.sensorId,temp)
            self.mqttClient.publish(self.mqttTopic,data,2)
            time.sleep(self.interval)

class ThermometerClient(object):

    def get_args(self):
        ap = argparse.ArgumentParser()
        ap.add_argument('-H','--host',help="MQTT Host to connect to", default='localhost')
        ap.add_argument('-p','--port',help="MQTT port to connect to", type=int, default='1883')
        ap.add_argument('-i','--identifier',help="Local identifier to send to server", default='test')
        ap.add_argument('-v', help="Increase log level, can be specified multiple times", action='count', default=1)
        ap.add_argument('-t','--topic',help="topic postfix to append after /surveillance/<type>/")
        args = ap.parse_args()
        return args

    def connect(self, args):
        client = paho.Client()
        client.connect(args.host, args.port, 60)
        return client

    def dump_args(self, args):
        logging.info("Starting with arguments:")
        ardict = args.__dict__
        for a in ardict:
            if ardict[a] is not None:
                logging.info("--%s = %s" % (a,ardict[a]))

    def main(self):
        #os.system('modprobe w1-gpio')
        #os.system('modprobe w1-therm')
        args = self.get_args()
        loglevel = logging.WARNING
        if args.v == 2:
            loglevel = logging.INFO
        elif (args.v > 2) or 'pdb' in sys.modules:
            loglevel = logging.DEBUG

        #Logger
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=loglevel)
        self.dump_args(args)

        self.base_dir = '/sys/bus/w1/devices/'
        sensors = self.device_folder = glob.glob(base_dir + '28*')
        sensor_files = dict()
        for s in sensors:
            logging.debug("adding sensor %s (%s)" % (os.path.basename(s),os.path.join(s,'w1_slave')))
            sensor_files[os.path.basename(s)] = os.path.join(s,'w1_slave')


        temperature_topic = "/surveillance/temperature/%s/%s" % (args.topic,args.identifier)
        surveillance_topic = "/surveillance/image/%s" % args.identifier
        protocol = ThermometerProtocol()
        client = self.connect(args)
        client.loop_start()

        reader_threads = []
        for s in sensor_files:
            logging.info("Starting thread for sensor %s" % s)
            t = ThermometerReader(args.identifier,s,sensor_files[s],60,client,temperature_topic)
            client.publish(temperature_topic,protocol.new_sensor(args.identifier,s),2)
            reader_threads.append(t)
            t.start()

        #Start camera if module is present
        t = CameraReader(client,surveillance_topic,args.identifier)
        reader_threads.append(t)
        t.start()

        try:
            while threading.active_count() > 0:
                time.sleep(60)
        finally:
            client.loop_stop()
            client.disconnect()

if __name__ == '__main__':
    t = ThermometerClient()
    t.main()
