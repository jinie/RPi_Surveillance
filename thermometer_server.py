#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
#import pdb
import json
import time
import shlex
import astral
from base64 import b64decode
import sqlite3
import logging
import rrdtool
import argparse
import datetime
import subprocess
from threading import Thread
import paho.mqtt.client as paho


class SurveillanceDatabase(object):
    _version = "1"
    _dbname = None
    _dbobject = None
    _instance = None

    _ddl = [
        'CREATE TABLE Sensors(id INTEGER PRIMARY KEY, host TEXT NOT NULL, sensor TEXT NOT NULL, alias TEXT, rrdGraph INTEGER DEFAULT 0, last_update INTEGER)',
        'CREATE UNIQUE INDEX SensorsIdx ON Sensors(host,sensor)',
        'CREATE TABLE Readings(id INTEGER PRIMARY KEY,host TEXT NOT NULL, sensorId TEXT NOT NULL, timestamp TEXT NOT NULL, reading TEXT)',
        'CREATE TABLE Surveillance(id INTEGER PRIMARY KEY, host TEXT NOT NULL, timestamp TEXT NOT NULL, imageLink TEXT NOT NULL)',
        'CREATE UNIQUE INDEX SurveillanceIdx ON Surveillance(imageLink)'
    ]

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def open(self, filename):
        self._dbname = filename
        try:
            self._dbobject = sqlite3.connect(filename)
            cur = self._dbobject.cursor()
            cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Readings'")
            row = cur.fetchone()
            if row[0] == 0:
                for s in self._ddl:
                    logging.debug(s)
                    cur.execute(s)
                self._dbobject.commit()
            cur.close()
            return self
        except Exception as e:
            logging.exception(e)
            sys.exit(-1)

    def __del__(self):
        if self._dbobject is not None:
            self._dbobject.close()
        self._dbobject = None

    def insert_reading(self, host, sensorId, timestamp, reading):
        cur = self._dbobject.cursor()
        cur.execute("INSERT INTO Readings(host, sensorId, timestamp, reading) VALUES(?, ?, ?, ?)", (host, sensorId,timestamp,reading))
        self._dbobject.commit()
        cur.close()

    def insert_surveillance(self, host, imageLink, timestamp):
        cur = self._dbobject.cursor()
        cur.execute("INSERT INTO Surveillance(host,timestamp,imageLink) VALUES(?, ?, ?)", (host,timestamp,imageLink))
        self._dbobject.commit()
        cur.close()

    def get_sensor(self, host, sensor):
        cur = self._dbobject.cursor()
        cur.execute("SELECT id FROM Sensors where host = ? and sensor = ?", (host,sensor))
        row = cur.fetchone()
        cur.close()
        if(row is not None):
            return row[0]
        else:
            return None

    def get_sensor_rrdGraph(self, host, sensor):
        cur = self._dbobject.cursor()
        cur.execute("SELECT rrdGraph FROM Sensors where host = ? and sensor = ?", (host,sensor))
        row = cur.fetchone()
        cur.close()
        if(row is not None):
            return row[0]
        else:
            return 0

    def get_graph_sensors_iter(self):
        cur = self._dbobject.cursor()
        cur.execute("SELECT host,sensor FROM Sensors where rrdGraph = 1")
        r = cur.fetchone()
        while r is not None:
            yield r
            r = cur.fetchone()
        cur.close()

    def get_sensor_name(self,host,sensor):
        cur = self._dbobject.cursor()
        cur.execute("SELECT sensor,alias from Sensors WHERE sensor=? and host=?", (sensor,host))
        row = cur.fetchone()
        cur.close()
        ret = sensor
        if(row is not None):
            if row[1] is not None:
                ret = row[1]
        return ret

    def register_sensor(self, host, sensor):
        cur = self._dbobject.cursor()
        cur.execute("INSERT INTO Sensors(host,sensor) VALUES(?,?)", (host,sensor))
        self._dbobject.commit()
        cur.close()

    def get_surveillance_dates_iter(self, host):
        cur = self._dbobject.cursor()
        cur.execute("SELECT DISTINCT DATE(timestamp) FROM Surveillance WHERE DATE(timestamp) <= DATE('now','-1 day')")
        r = cur.fetchone()
        while r is not None:
            yield r
            r = cur.fetchone()
        cur.close()

    def get_surveillance_files_for_date(self,date,host):
        cur = self._dbobject.cursor()
        cur.execute("SELECT imageLink from Surveillance WHERE DATE(timestamp) = ? AND host=?", (date,host))

        r = cur.fetchall()
        cur.close()
        return r

    def get_surveillance_hosts_iter(self):
        cur = self._dbobject.cursor()
        cur.execute("SELECT DISTINCT host FROM Surveillance")
        r = cur.fetchone()
        while r is not None:
            yield r
            r = cur.fetchone()
        cur.close()

    def delete_surveillance_files(self,host,date):
        cur = self._dbobject.cursor()
        cur.execute("DELETE FROM Surveillance WHERE DATE(timestamp) = ? AND host=?",(date,host))
        self._dbobject.commit()
        cur.close()

    def update_last_update(self,host,sensor,date):
        cur = self._dbobject.cursor()
        cur.execute("UPDATE Sensors SET last_update=? WHERE host=? AND sensor=?",(date,host,sensor))
        self._dbobject.commit()
        cur.close()

    def check_last_update(self,max_time=600):
        cur = self._dbobject.cursor()
        cur.execute("SELECT host,sensor,last_update FROM sensors where last_update < ?",(int(time.time()-max_time),))
        rows = cur.fetchall()
        cur.close()
        return rows

    def update_notification_sent(self, host, sensor, value=1):
        cur = self._dbobject.cursor()
        cur.execute("UPDATE Sensors SET notification_sent=? WHERE host=? AND sensor=?",(value,host,sensor))
        cur.close()

class ThermometerProtocol(object):

    def new_reading(self, sensor, reading, timestamp=datetime.datetime.now()):
        data = {'reading':{'sensor':sensor, 'reading':reading, 'timestamp':timestamp}}
        m = self._new_message(payload=data)
        return json.dumps(m)

    def new_image(self, host, image, timestamp=None):
        if timestamp is None:
            timestamp = str(datetime.datetime.now())

        data = {'surveillance':{'host':host, 'image':image, 'timestamp':timestamp}}
        return json.dumps(data)

    def new_notification(self, notification):
        data = {'notification':notification}
        return json.dumps(data)

    def parse_message(self, message):
        dataDict = json.loads(message)
        return dataDict


class TimelapseCreator(Thread):

    def __init__(self, imagePath, timelapsePath, databasePath):
        super(TimelapseCreator, self).__init__()
        self.daemon = True
        self.imagePath = imagePath
        self.timelapsePath = timelapsePath
        self.databasePath = databasePath

    def create_timelapse(self):
        for host in self.database.get_surveillance_hosts_iter():
            h = host[0]
            for date in self.database.get_surveillance_dates_iter(h):
                d = date[0]
                logging.debug(d)
                logging.debug(h)
                #pdb.set_trace()
                images = self.database.get_surveillance_files_for_date(d,h)
                mfile = os.path.join(self.imagePath,"%s_%s_surveillance_files.txt" % (h,d))
                with open(mfile,"wt") as f:
                    for img in images:
                        f.write(str(img[0])+"\n")
                try:
                    out = shlex.split("/usr/bin/mencoder -nosound -ovc lavc -lavcopts vcodec=mpeg4:aspect=16/9:vbitrate=8000000 -vf scale=2592:1944 -o '%s/%s_%s.avi' -mf type=jpeg:fps=24 mf://@%s_%s_surveillance_files.txt" % (self.timelapsePath,h,d,h,d))
                    p = subprocess.Popen(out,cwd=self.imagePath)
                    (stdoutdata,stdindata) = p.communicate()
                    logging.debug(stdoutdata)
                    os.unlink(os.path.join(self.imagePath,"%s_%s_surveillance_files.txt") % (h,d))
                    for img in images:
                        try:
                            i = img[0]
                            ipath = os.path.join(self.imagePath,i)
                            if(os.path.exists(ipath)):
                                os.unlink(ipath)
                        except Exception as e:
                            logging.exception(e)
                    self.database.delete_surveillance_files(h,d)
                except Exception as e:
                    logging.exception(e)

    def clean_timelapse(self,max_age=604800):
        oldest = (time.time() // 86400) - max_age
        for root,dirs,files in os.walk(self.timelapsePath):
            for f in files:
                name,ext = os.path.splitext(f)
                if ext.lower() != '.avi':
                    continue
                if (os.path.getmtime(os.path.join(root,f)) // 86400) < oldest:
                    os.unlink(os.path.join(root,f))

    def run(self):
        self.database = SurveillanceDatabase().open(self.databasePath)
        while True:
            try:
                self.create_timelapse()
                self.clean_timelapse()
                time.sleep(3600)
            except Exception as e:
                logging.exception(e)


class RRDGraphCreator(Thread):

    def __init__(self, rrdPath, rrdImagePath, databasePath, mqttClient=None):
        super(RRDGraphCreator, self).__init__()
        self.daemon = True
        self.rrdPath = rrdPath
        self.rrdImagePath = rrdImagePath
        self.databasePath = databasePath
        self.protocol = ThermometerProtocol()
        self.mqttClient = mqttClient

    def run(self):
        self.database = SurveillanceDatabase()
        self.database.open(self.databasePath)
        while True:
            logging.info("Checking last_update")
            rows = self.database.check_last_update()
            for r in rows:
                n = self.protocol.new_notification("Sensor %s on host %s, last update %s" % (r[0],r[1],r[2]))
                self.database.update_notification_sent(r[0],r[1])
                self.mqttClient.publish('/surveillance/notification/%s/temperature/alert' % (r[0],),n,2)

            logging.info("Updating RRD Graphs")
            self.create_rrd_graph()
            logging.debug("Done updating RRD Graphs")
            time.sleep(600)

    def sundata(self):
        a = astral.Astral()
        a.solar_depression = 'civil'
        sun = a['Copenhagen'].sun(datetime.datetime.now(),local=True)
        dusk = sun['dusk']
        dawn = sun['dawn']
        sunrise = sun['sunrise']
        sunset = sun['sunset']
        #pdb.set_trace()
        dusks = int(abs((dusk - dusk.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()))
        dawns = int(abs((dawn - dawn.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()))
        sunss = int(abs((sunset - sunset.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()))
        sunrs = int(abs((sunrise - sunrise.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()))
        return sunrs,sunss,dusks,dawns

    def create_rrd_graph(self):
        defs = []
        logging.debug("getting sensors from database")
        colors = ['#FF0000','#00DC00','#00FF00','#0000FF','#8F4F00']
        cur_color = 0
        sunr,suns,dusk,dawn = self.sundata()
        suninf = False
        defs.append('COMMENT:Location\\t    Last\\t\\tAvg\\t\\tMax\\t\\tMin\\n')
        defs.append('HRULE:0#0000FF:freezing\\n')
        for host,sensor in self.database.get_graph_sensors_iter():
            logging.debug("Get sensor name : %s,%s" % (host,sensor))
            sensor_name = self.database.get_sensor_name(host,sensor)
            rrdfile = os.path.join(self.rrdPath,"%s_%s_temperature.rrd" % (host,sensor))
            #defs.append('COMMENT:\\u')
            defs.append("DEF:%s=%s:a:AVERAGE" % (sensor,rrdfile))
            defs.append("LINE2:%s%s:%s\\t" % (sensor,colors[cur_color],sensor_name))
            defs.append('GPRINT:{0}:LAST:%5.1lf °C\\t'.format(sensor))
            defs.append('GPRINT:{0}:AVERAGE:%5.1lf °C\\t'.format(sensor))
            defs.append('GPRINT:{0}:MAX:%5.1lf °C\\t'.format(sensor))
            defs.append('GPRINT:{0}:MIN:%5.1lf °C\\n'.format(sensor))
            if suninf is False:
                defs.append('CDEF:nightplus=LTIME,86400,%,{0},LT,INF,LTIME,86400,%,{1},GT,INF,UNKN,{2},*,IF,IF'.format(sunr,suns,sensor))
                defs.append('CDEF:nightminus=LTIME,86400,%,{0},LT,NEGINF,LTIME,86400,%,{1},GT,NEGINF,UNKN,{2},*,IF,IF'.format(sunr,suns,sensor))
                defs.append('AREA:nightplus#E0E0E0')
                defs.append('AREA:nightminus#E0E0E0')
                defs.append('CDEF:dusktill=LTIME,86400,%,{0},LT,INF,LTIME,86400,%,{1},GT,INF,UNKN,{2},*,IF,IF'.format(dawn,dusk,sensor))
                defs.append('CDEF:dawntill=LTIME,86400,%,{0},LT,NEGINF,LTIME,86400,%,{1},GT,NEGINF,UNKN,{2},*,IF,IF'.format(dawn,dusk,sensor))
                defs.append('AREA:dusktill#CCCCCC')
                defs.append('AREA:dawntill#CCCCCC')
                suninf = True

            cur_color = cur_color + 1 if cur_color +1 < len(colors) else 0
        try:
            logging.info("Generating RRD Graphs")
            rrdtool.graph(os.path.join(self.rrdImagePath,"temperature-hour.png" ),
                        "--start","-6h",
                        "-u","35",
                        "-l","-10",
                        "--full-size-mode",
                        "--width","700",
                        "--height","400",
                        "--slope-mode",
                        "--color","SHADEB#9999CC",
                        defs
                          )

            rrdtool.graph(os.path.join(self.rrdImagePath,"temperature-day.png" ),
                        "--start","-1d",
                        "--end","now",
                        "-u","35",
                        "-l","-10",
                        "-v","Last 24 hours",
                        "--full-size-mode",
                        "--width","700",
                        "--height","400",
                        "--slope-mode",
                        "--color","SHADEB#9999CC",
                        defs
                          )

            rrdtool.graph(os.path.join(self.rrdImagePath,"temperature-week.png" ),
                        "--start","-1w",
                        "--end","now",
                        "-u","35",
                        "-l","-10",
                        "-v","Last week",
                        "--full-size-mode",
                        "--width","700",
                        "--height","400",
                        "--slope-mode",
                        "--color","SHADEB#9999CC",
                         defs
                          )

            rrdtool.graph(os.path.join(self.rrdImagePath,"temperature-month.png" ),
                        "--start","-1m",
                        "--end","now",
                        "-u","35",
                        "-l","-10",
                        "-v","Last month",
                        "--full-size-mode",
                        "--width","700",
                        "--height","400",
                        "--slope-mode",
                        "--color","SHADEB#9999CC",
                         defs
                          )

            rrdtool.graph(os.path.join(self.rrdImagePath,"temperature-year.png" ),
                        "--start","-1y",
                        "--end","now",
                        "-u","35",
                        "-l","-10",
                        "-v","Last year",
                        "--full-size-mode",
                        "--width","700",
                        "--height","400",
                        "--slope-mode",
                        "--color","SHADEB#9999CC",
                        defs
                          )

        except Exception as e:
            logging.exception(e)


class ThermometerServer(object):
    protocol = None
    lastmin = 99999
    lastmax = -99999

    def __init__(self):
        self.protocol = ThermometerProtocol()

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe("/surveillance/#",2)

    def on_message(self, client, userdata, msg):
        logging.info("received message with topic"+msg.topic)
        try:
            msgData = msg.payload.decode('utf-8')
            dataDict = self.protocol.parse_message(msgData)
            database = self.database

            if('register_sensor' in dataDict):
                logging.debug('received request to register new sensor')
                sens = dataDict['register_sensor']
                if(database.get_sensor(sens['host'],sens['sensor']) is None):
                    logging.info("registering new sensor with host %s, sensor %s" % (sens['host'],sens['sensor']))
                    database.register_sensor(sens['host'],sens['sensor'])
                    self.setup_rrd(sens['host'],sens['sensor'])

            elif('reading' in dataDict):
                reading = dataDict['reading']
                host = reading['host']
                sensor = reading['sensor']
                logging.debug(reading)

                #database.insert_reading(reading['host'],reading['sensor'],reading['timestamp'],reading['reading'])
                database.update_last_update(int(time.time()),reading['host'],reading['sensor'])
                self.update_rrd(host,sensor,reading['reading'],reading['timestamp'])
                self.check_notification(host,sensor,reading['reading'],userdata,client)

            elif('surveillance' in dataDict):
                surv = dataDict['surveillance']
                img = b64decode(surv['image'].encode('ascii'))
                fname = 'surveillance_%s_%s.jpeg' % (surv['host'],surv['timestamp'])
                with open(os.path.join(self.surveillanceImagePath,fname),'wb') as f:
                    f.write(img)
                database.insert_surveillance(surv['host'],fname,surv['timestamp'])
        except Exception as e:
            logging.exception(e)


    def on_discconect(self, client, userdata, rc):
        client.reconnect()


    def check_notification(self, host, sensor, reading, userdata, client):
        threshold_temp = None
        last_alert = None
        if userdata.alertsensor is None:
            return
        sname = SurveillanceDatabase.get_instance().get_sensor_name(host,sensor)
        topic_suffix = None
        if sname not in userdata.alertsensor:
            return
        if userdata.alertmin is not None:
            #pdb.set_trace()
            if (reading < userdata.alertmin) and (reading < (self.lastmin-1.0)):
                threshold_temp = userdata.alertmin
                last_alert = self.lastmin
                self.lastmin = float(reading)
                topic_suffix = 'minimum'
            elif reading > userdata.alertmin:
                self.lastmin = 99999

        if userdata.alertmax is not None:
            if (reading > userdata.alertmax) and (reading > (self.lastmax+1.0)):
                threshold_temp = userdata.alertmax
                last_alert = self.lastmax
                self.lastmax = float(reading)
                topic_suffix = 'maximum'
            elif reading < userdata.alertmax:
                self.lastmax = -99999

        if threshold_temp is not None:
            #pdb.set_trace()
            logging.info("threshold reached, %f < %f (last %f)" % (reading, threshold_temp, last_alert))
            d = self.protocol.new_notification("sensor %s on host %s has reached %02.2fC" % (sname,host,reading))
            client.publish('/surveillance/notification/%s/temperature/%s' % (host,topic_suffix),d,2)


    def setup_rrd(self, host, sensor):
        try:
            logging.info("creating rrd database for %s:%s" % (host,sensor))
            rrdFile = os.path.join(self.rrdPath,"%s_%s_temperature.rrd" % (host,sensor))
            rrdtool.create(rrdFile,"--start","now","--step","60","DS:a:GAUGE:120:-50:50","RRA:AVERAGE:0.5:2:720","RRA:AVERAGE:0.5:15:672","RRA:AVERAGE:0.5:60:720","RRA:AVERAGE:0.5:360:1460")
        except Exception as e:
            logging.exception(e)

    def update_rrd(self, host, sensor, reading, timestamp=None):
        template = "%s" % "a"
        update = "N:%f" % reading
        if timestamp is not None:
            try:
                update = "%s:%f" % (datetime.datetime.strptime(timestamp,"%Y-%m-%d %H:%M:%S.%f").strftime("%s"),reading)
            except Exception as e:
                logging.exception(e)

        rrdfile = os.path.join(self.rrdPath,"%s_%s_temperature.rrd" % (host,sensor))
        logging.debug("RRD update : %s, %s" % (template,update))
        rrdtool.update(rrdfile ,"--template",template, update)


    def setup(self, prefix):
        self.databasePath = os.path.join(prefix,"database")
        self.databaseFile = os.path.join(self.databasePath,"surveillance.sqlite3")
        self.rrdPath = os.path.join(prefix,"rrd")
        self.imagePath = os.path.join(prefix,"images")
        self.surveillanceImagePath = os.path.join(self.imagePath,"surveillance")
        self.rrdImagePath = os.path.join(self.imagePath,"rrd")
        self.timelapsePath = os.path.join(self.imagePath,"timelapse")

        for p in (self.databasePath, self.rrdPath, self.imagePath, self.surveillanceImagePath, self.rrdImagePath, self.timelapsePath):
            logging.info("PATH:"+p)
            if not os.path.exists(p):
                os.makedirs(p)

        self.database.open(self.databaseFile)

    def get_args(self):
        ap = argparse.ArgumentParser()
        ap.add_argument('-H','--host',help="MQTT Host to connect to", default='localhost')
        ap.add_argument('-p','--port',help="MQTT port to connect to", type=int, default='1883')
        ap.add_argument('-P','--prefix',help="Prefix to where data should be stored", default='/mnt/data/surveillance')
        ap.add_argument('-m','--alertmin',help="Send notification when temperature goes below this value", type=int)
        ap.add_argument('-M','--alertmax',help="Send notification when temperature goes above this value", type=int)
        ap.add_argument('-s','--alertsensor',help="Sensor to monitor for alerts",nargs='+')
        ap.add_argument('-v', help="Increase log level, can be specified multiple times", action='count', default=1)
        args = ap.parse_args()
        return args

    def dump_args(self, args):
        logging.info("Starting with arguments:")
        ardict = args.__dict__
        for a in ardict:
            if ardict[a] is not None:
                logging.info("--%s = %s" % (a,ardict[a]))

    def main(self):
        args = self.get_args()
        self.database = SurveillanceDatabase.get_instance()
        self.setup(args.prefix)
        loglevel = logging.INFO
        if args.v == 2:
            loglevel = logging.INFO
        elif (args.v > 2) or 'pdb' in sys.modules:
            loglevel = logging.DEBUG
        #Logger
        root = logging.getLogger()
        root.setLevel(loglevel)
        #stdout handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(loglevel)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)
        #file handler
        ch = logging.FileHandler(os.path.join(args.prefix,'thermometer_server.log'))
        ch.setLevel(loglevel)
        ch.setFormatter(formatter)
        root.addHandler(ch)

        self.dump_args(args)

        client = paho.Client()
        client.user_data_set(args)
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.on_discconect = self.on_discconect
        client.connect(args.host, args.port, 60)
        RRDGraphCreator(self.rrdPath, self.rrdImagePath, self.databaseFile, client).start()
        TimelapseCreator(self.surveillanceImagePath, self.timelapsePath,self.databaseFile).start()
        client.loop_forever()

if __name__ == '__main__':
    t = ThermometerServer()
    t.main()
