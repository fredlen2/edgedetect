# !/usr/local/bin/python3.7

import os
import sys
import json
import warnings
import argparse
import datetime
import time
import shutil
import threading
import requests
import hashlib
import logging
from pytz import timezone
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
# from watchdog.events import LoggingEventHandler
from requests.auth import HTTPBasicAuth

from tramscore import Tramscore
from tramscore.recognize import FileRecognizer

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(process)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

MEDIA_STATION_DIR_NAME = "starr_fm"


class RecognizeStream(Tramscore):
    new_ad_name = ""
    MEDIA_STATION_NAME = "STAR FM"
    MEDIA_STATION_ID = 15

    def __init__(self, ad_name):
        self.ad_name = ad_name

        logger.info("Recognizer system initializing config...")
        self.config = {
            "database": {
                "host": "127.0.0.1",
                "user": "root",
                # "password": "testP@55w0rd",
                # "database": "test_trams_db"
                "passwd": "testP@55w0rd",
                "db": "test_trams_db"
            },

            "database_type": "mysql"
        }
        self.trams = Tramscore(self.config)
        if self.trams:
            logger.info("initializing test ok")

    # def core_init(self):
    #     config = {
    #         "database": {
    #             "host": "127.0.0.1",
    #             "user": "root",
    #             "passwd": "testP@55w0rd",
    #             "db": "test_trams_db"
    #         },
    #
    #         "database_type": "mysql"
    #     }
    #     # "connect_timeout": "1000000"
    #     trams = Tramscore(config)
    #     return trams

    def fred_shuttle(self, advert, path_to_dest):
        try:
            logger.info("trying fred_shuttle call...")
            shutil.move(advert, path_to_dest)
            logger.info("fred_shuttle worked.")
        except Exception as err:
            logger.error("ERROR in Fred Shuttle: %s" % err)

    def printit(self):
        # Current time in UTC
        detect_date = "%A, %B %d, %Y"
        detect_time = "%H:%M:%S%p"
        now_utc = datetime.datetime.now(timezone('UTC'))

        threading.Timer(3600.0, printit).start()

        print("**************************************************\n")
        print("%s was not detected as at %s, on %s while listening to %s!") \
        % (self.ad_name, now_utc.strftime(detect_time), now_utc.strftime(detect_date), self.MEDIA_STATION_NAME)
        print("\n**************************************************")

    def recognize(self):
        # trams = self.core_init()
        advert_result = self.trams.recognize(FileRecognizer, self.ad_name)

        # fmt = "%Y-%m-%d %H:%M:%S%p %Z%z"
        format_detect_date = "%Y-%m-%d"
        detect_date = "%a, %b %d, %Y"
        detect_time = "%H:%M:%S"
        filename = "%Y%b%d-%H%M%S"

        # extract file name and file extension
        file_name, file_extension = os.path.splitext(self.ad_name)

        # Current time in UTC
        now_utc = datetime.datetime.now(timezone('UTC'))
        self.new_ad_name = self.ad_name + "%s.%s" % (now_utc.strftime(filename), file_extension[1:])

        try:
            if advert_result is not None:
                advert_rel_conf = float(round(advert_result["relative_confidence"], 2))

                if advert_rel_conf > 3.00 and advert_result["confidence"] > 100:

                    advert_result["advert_name"] = (advert_result["advert_name"]).decode('utf-8')
                    advert_result["match_time"] = round(advert_result["match_time"], 2)
                    advert_result["relative_confidence"] = advert_rel_conf
                    advert_result["file_sha1"] = advert_result["file_sha1"]
                    advert_result["format_date_detected"] = now_utc.strftime(format_detect_date)
                    advert_result["date_detected"] = now_utc.strftime(detect_date)
                    advert_result["time_detected"] = now_utc.strftime(detect_time)
                    advert_result["client_user_id"] = int(advert_result["client_user_id"])

                    for media_id in advert_result["mediastation_id"]:
                        if int(media_id) == self.MEDIA_STATION_ID:
                            advert_result["mediastation_id"] = self.MEDIA_STATION_ID

                            try:
                                logger.info("Request data: %s" % advert_result)
                                # Log response on command line
                                self.cmd_response(advert_result)

                                # post request to tramsweb api to save recognition results
                                self.recognition_post(advert_result)

                                # perform adroll request
                                #     try:
                                #         campaign_status = self.get_campaign_handler_single(apikey, eid)
                                #         print("Current Adroll campaign status is: %s" % campaign_status)
                                #
                                #         time.sleep(1)
                                #         if campaign_status == "paused":
                                #             self.put_campaign_handler_live(apikey, eid)
                                #         else:
                                #             self.put_campaign_handler_live(apikey, eid)
                                #
                                #     except (TypeError, RuntimeError) as err:
                                #         print("Sorry the social media campaign failed!\n")

                                # wait 1 sec and move recognized file to log folder
                                # time.sleep(1)
                                new_advert_name = (advert_result["advert_name"] + "%s.%s") % \
                                                  (now_utc.strftime(filename), file_extension[1:])
                                os.rename(self.ad_name, new_advert_name)
                                logger.info("1. Mo - detected")
                                shutil.move(new_advert_name, detected_path)
                                # self.fred_shuttle(new_advert_name, detected_path)
                            except Exception as err:
                                logger.error("ERROR IN DETECTED: %s" % err)
                                logger.info("2. Mo - error while detected")
                                self.fred_shuttle(self.ad_name, failed_path)
                            break

                    else:
                        logger.info("3. Mo - No media stations detected")
                        self.fred_shuttle(self.ad_name, failed_path)

                else:
                    logger.info("4. Mo detected RC low")
                    self.fred_shuttle(self.ad_name, failed_path)

            else:
                logger.info("5. Mo None")
                self.fred_shuttle(self.ad_name, failed_path)

        # except (TypeError, RuntimeError, AttributeError, KeyError, OSError) as err:
        except Exception as err:
            logger.info("6. Mo General error")
            self.fred_shuttle(self.ad_name, failed_path)
            logger.error("ERROR ON RECOGNITION ATTEMPT: %s" % err)

    def recognition_post(self, ad_results):
        # auth = HTTPBasicAuth('equansah@mtechcomm.com', 'Birthday2010@')

        url = "https://test.blazoninnovations.com.gh/api/v1/recognitions/"
        client_id_url = "https://test.blazoninnovations.com.gh/api/v1/clients/%s/" % ad_results['client_user_id']
        media_station_url = "https://test.blazoninnovations.com.gh/api/v1/stations/%s/" % self.MEDIA_STATION_ID

        prep_data = {
            "client_user": client_id_url, "media_station": media_station_url,
            "advert_id": ad_results['advert_id'],
            "advert_name": ad_results['advert_name'], "confidence": ad_results['confidence'],
            "relative_confidence": ad_results['relative_confidence'], "match_time": ad_results['match_time'],
            "file_sha1": ad_results['file_sha1'], "audio_length": ad_results['audio_length'],
            "offset_seconds": ad_results['offset_seconds'], "offset": ad_results['offset'],
            "date_detected": ad_results['format_date_detected'],
            "time_detected": ad_results['time_detected']
        }

        # logger.info("\n*****************\n Prep data: %s \n*****************\n" % prep_data)

        response = requests.post(url, data=prep_data, verify=False)
        logger.info("RECOGNITION UPDATE: Response code: %s and Reason: %s." % (response.status_code, response.reason))

    def send_sms(self, msg, phone, sender):
        sender = sender  # Sender Name
        recipient = phone  # Recipient MSISDN
        message = msg  # Message to Send
        apikey = "a0b195e7cba5b446eddaad70b7ef8ad4"  # api provided by Mtech Communications

        # Find md5 hash for the first three parameters
        extra = hashlib.md5((sender + recipient + message).encode("utf-8")).hexdigest()

        url = "http://176.58.113.107/send.php"
        prep_data = {'from': sender, 'to': recipient, 'message': message, 'apikey': apikey, 'extra': extra}

        response = requests.post(url, data=prep_data)
        logger.info("SMS: Response code: %s and Reason: %s." % (response.status_code, response.reason))

    def get_campaign_handler_single(self, apikey, eid):
        apikey = apikey  # API Key provided by Adroll
        eid = eid  # Campaign eid from Adroll
        auth = HTTPBasicAuth('equansah@mtechcomm.com', 'Birthday2010@')

        url = "https://services.adroll.com/activate/api/v2/campaign?apikey=%s&eid=%s" % (apikey, eid)
        response = requests.get(url, auth=auth)
        response = response.json()

        return response["data"][0]["status"]

    def put_campaign_handler_pause(self, apikey, eid):
        apikey = apikey  # API Key provided by Adroll
        eid = eid  # Campaign EID from Adroll

        auth = HTTPBasicAuth('equansah@mtechcomm.com', 'Birthday2010@')

        url = "https://services.adroll.com/activate/api/v2/campaign?apikey=%s&eid=%s" % (apikey, eid)
        prep_data = {'status': 'paused'}

        response = requests.put(url, json=prep_data, auth=auth)
        logger.info(
            "ADROLL CAMPAIGN PAUSE: Response code: %s and Reason: %s." % (response.status_code, response.reason))
        # logger.info("Campaign paused")

    def put_campaign_handler_live(self, apikey, eid):
        apikey = apikey  # API Key provided by Adroll
        eid = eid  # Campaign EID from Adroll

        auth = HTTPBasicAuth('equansah@mtechcomm.com', 'Birthday2010@')

        url = "https://services.adroll.com/activate/api/v2/campaign?apikey=%s&eid=%s" % (apikey, eid)
        prep_data = {'status': 'live'}

        response = requests.put(url, json=prep_data, auth=auth)
        logger.info("ADROLL CAMPAIGN LIVE: Response code: %s and Reason: %s." % (response.status_code, response.reason))
        # logger.info("Campaign is live now")

    def cmd_response(self, advert_result):
        logger.info("------------------------------------------------------------------------------")
        logger.info("\n%s has been detected on %s today, %s at %s.\nTime taken to detect this advert was %sseconds "
                    "with an accuracy of %spercent.\n"
                    % (advert_result['advert_name'], self.MEDIA_STATION_NAME, advert_result['date_detected'],
                       advert_result['time_detected'], advert_result['match_time'],
                       advert_result["relative_confidence"]))
        logger.info("------------------------------------------------------------------------------")

    def sms_response(self, advert_result, client_name):
        ad_msg = "Hello %s, Test advert %s is being played now, %s at %s on %s. " \
                 "Test advert was detected in %sseconds." \
                 % (client_name, advert_result['advert_name'], advert_result['date_detected'],
                    advert_result['time_detected'], self.MEDIA_STATION_NAME, advert_result['match_time'])
        return ad_msg

    def sms_response_old(self, advert_result, client_name):
        percent = '%'
        ad_msg = "Hello %s, %s has been played today, %s at %s on %s. " \
                 "Test advert was detected in %sseconds with %s%s accuracy." \
                 % (client_name, advert_result['advert_name'], advert_result['date_detected'],
                    advert_result['time_detected'], self.MEDIA_STATION_NAME,
                    advert_result['match_time'], advert_result["relative_confidence"], percent)
        return ad_msg


class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.mp3", "*.mp4", "*.wav", "*.mov", "*.aac"]

    def process(self, event):
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        recognize_stream = RecognizeStream(event.src_path)
        recognize_stream.recognize()
        logger.info("Trying detection on: %s" % event.src_path)

    def on_created(self, event):
        time.sleep(8)
        self.process(event)


if __name__ == '__main__':

    args = sys.argv[1:]

    media_storage = "/home/blazoninnovation/testbed/test_streams/ghana/radio/%s_stream_watcher/" % MEDIA_STATION_DIR_NAME
    detected_path = "/home/blazoninnovation/testbed/test_streams/ghana/radio/%s_detected_logs/" % MEDIA_STATION_DIR_NAME
    failed_path = "/home/blazoninnovation/testbed/test_streams/ghana/radio/%s_failed_logs/" % MEDIA_STATION_DIR_NAME

    apikey = "slGir7tG967figsHx39Nw6oJGxe3NSUj"  # API Key provided by Adroll
    eid = "KKJY7PQRMJBUNM44SJ8VOL"  # Kum Campaign EID from Adroll

    try:
        os.stat(media_storage)
        os.stat(detected_path)
        os.stat(failed_path)
    except:
        os.mkdir(media_storage)
        os.mkdir(detected_path)
        os.mkdir(failed_path)

    if len(args) == 2:
        destpath = args[1] if args else destpath

    # section to start monitoring
    observer = Observer()
    observer.schedule(MyHandler(), path=args[0] if args else media_storage)

    try:

        observer.start()

        while True:
            time.sleep(1)
        # observer.join()
    except:
        logger.error("\nERROR: Ouch! Sorry for the glitch :) ...")
        observer.stop()

    observer.join()

    # finally:
    #     observer.start()
    #
    #     while True:
    #         time.sleep(1)
    #     observer.join()

    # observer.join(timeout=10.0)
    # observer.isAlive()

    # finally:
    #     observer = Observer()
    #     observer.schedule(MyHandler(), path=args[0] if args else media_storage)
    #     observer.start()
    #
    #     while True:
    #         time.sleep(1)
    #
    #     observer.join(timeout=5.0)
    #     # observer.isAlive()
