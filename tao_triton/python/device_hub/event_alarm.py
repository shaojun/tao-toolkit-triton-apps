import _thread
import datetime
import threading
import time
from enum import Enum
from typing import List
import tao_triton.python.device_hub.event_detectors.event_detector as event_detector
from tao_triton.python.device_hub.event_detectors.electric_bicycle_entering_event_detector import ElectricBicycleEnteringEventDetector

import requests


class EventAlarmPriority(Enum):
    VERBOSE = 9
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    FATAL = 4
    CLOSE = 5


class EventAlarm:
    def __init__(self, event_detector, original_utc_timestamp: datetime,
                 priority: EventAlarmPriority,
                 description: str, code="", data={}, imageStr=""):
        """

        @type event_detector: EventDetectorBase
        """
        self.event_detector = event_detector
        self.priority = priority
        self.description = description
        # the alarm may be triggered at remote(board) side, this timestamp is from remote(board)
        self.original_utc_timestamp = original_utc_timestamp
        self.code = code
        self.data = data
        self.imageText = imageStr


class EventAlarmNotifierBase:
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        pass


class EventAlarmDummyNotifier(EventAlarmNotifierBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        if not alarms:
            return
        for a in alarms:
            pass
            # self.logger.info(
            #     "board: {}, Notifying alarm(by {}) with priority: {} -> {}".format(
            #         a.event_detector.timeline.board_id,
            #         a.event_detector.__class__.__name__,
            #         a.priority, a.description))


class EventAlarmWebServiceNotifier:
    # URL = "http://49.235.97.14:8801/warning"
    # URL = "http://36.138.48.162:8801/warning"
    URL = "https://api.glfiot.com/warning"
    URL_UPDATE = "https://api.glfiot.com/yunwei/warning/updatewarningmessagestatus"
    URL_Gather = "https://api.glfiot.com/api/apiduijie/postliftsGathering"
    # URL = "http://49.235.35.248:8028/warning"
    # URL_UPDATE = "http://49.235.35.248:8028/yunwei/warning/updatewarningmessagestatus"
    # URL_Gather = "http://49.235.35.248:8028/api/apiduijie/postliftsGathering"
    HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, logging):
        self.logger = logging.getLogger("eventAlarmWebServiceNotifierLogger")
        self.alarms = []
        self.infos = []
        _thread.start_new_thread(self.processAlarms, ())
        _thread.start_new_thread(self.processInfos, ())

    def notify(self, alarms: List[EventAlarm]):
        if not alarms or len(alarms) == 0:
            return
        for alarm in alarms:
            if alarm.code == "general_data" or alarm.code == "DOOROPEN" or alarm.code == "DOORCLOSE":
                self.infos.append(alarm)
            else:
                self.alarms.append(alarm)
        # self.alarms.extend(alarms)

    def processAlarms(self):
        while True:
            targert_alarms = None
            if self.alarms and len(self.alarms) > 0:
                targert_alarms = self.alarms.pop(0)
            if not targert_alarms:
                time.sleep(1)
            elif targert_alarms.code == "general_data":
                temp_data = targert_alarms
                try:
                    self.logger.info("board: {}, upload gerneral data from {}".format(
                        temp_data.event_detector.timeline.board_id,
                        temp_data.event_detector.__class__.__name__))
                    upload_response = requests.post(EventAlarmWebServiceNotifier.URL_Gather,
                                                    headers=EventAlarmWebServiceNotifier.HEADERS,
                                                    data=None,
                                                    json=temp_data.data)
                    if upload_response.status_code != 200 or upload_response.json()["code"] != 200:
                        self.logger.error("board: {}, liftId:{},Notify alarm from {} got error: {}".format(
                            temp_data.event_detector.timeline.board_id,
                            temp_data.event_detector.timeline.liftId,
                            temp_data.event_detector.__class__.__name__, upload_response.text[:500]))
                except:
                    self.logger.exception(
                        "board: {}, general data upload from {} got exception.".format(
                            temp_data.event_detector.timeline.board_id,
                            temp_data.event_detector.__class__.__name__))
            elif targert_alarms.priority == EventAlarmPriority.CLOSE:
                temp_alarm = targert_alarms
                try:
                    self.logger.info(
                        "board: {}, close alarm from {}".format(
                            temp_alarm.event_detector.timeline.board_id,
                            temp_alarm.event_detector.__class__.__name__))
                    put_response = requests.put(EventAlarmWebServiceNotifier.URL_UPDATE,
                                                headers=EventAlarmWebServiceNotifier.HEADERS,
                                                json={"liftId": temp_alarm.event_detector.timeline.liftId,
                                                      "type": temp_alarm.code,
                                                      "warningMessageId": "",
                                                      "base64string": ""})
                    if put_response.status_code != 200 or put_response.json()["code"] != 200:
                        self.logger.error(
                            "board: {}, Notify alarm from {} got error: {}".format(
                                temp_alarm.event_detector.timeline.board_id,
                                temp_alarm.event_detector.__class__.__name__,
                                put_response.text[:500]))
                except:
                    self.logger.exception(
                        "board: {}, close alarm from {} got exception.".format(
                            temp_alarm.event_detector.timeline.board_id,
                            temp_alarm.event_detector.__class__.__name__))
            else:
                post_data = None
                target_alarm = targert_alarms
                if target_alarm.event_detector.__class__.__name__ == ElectricBicycleEnteringEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "007",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "base64string": target_alarm.imageText,
                                 "data": target_alarm.data}
                elif target_alarm.event_detector.__class__.__name__ == event_detector.GasTankEnteringEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "0021",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "base64string": target_alarm.imageText,
                                 "data": target_alarm.data}
                elif target_alarm.event_detector.__class__.__name__ == event_detector.DoorOpenedForLongtimeEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "008",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "base64string": target_alarm.imageText,
                                 "data": target_alarm.data}
                elif target_alarm.event_detector.__class__.__name__ == event_detector.DoorRepeatlyOpenAndCloseEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "004",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "base64string": target_alarm.imageText,
                                 "data": target_alarm.data}
                else:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id,
                                 "warning_type": target_alarm.code,
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "base64string": target_alarm.imageText,
                                 "data": target_alarm.data}
                try:
                    self.logger.info(
                        "board: {}, Notifying alarm(by {}) with priority: {} at: {} -> {}".format(
                            target_alarm.event_detector.timeline.board_id,
                            target_alarm.event_detector.__class__.__name__,
                            target_alarm.priority, str(target_alarm.original_utc_timestamp), target_alarm.description))

                    # level: Debug=0, Info=1, Warning=2, Error=3, Fatal=4
                    post_response = requests.post(EventAlarmWebServiceNotifier.URL,
                                                  headers=EventAlarmWebServiceNotifier.HEADERS,
                                                  data=None,
                                                  json=post_data)
                    if post_response.status_code != 200:
                        self.logger.error(
                            "board: {}, Notify alarm from {} got error: {}".format(
                                target_alarm.event_detector.timeline.board_id,
                                target_alarm.event_detector.__class__.__name__,
                                post_response.text[:500]))
                    else:
                        pass
                        # self.logger.debug(
                        #     "board: {}, Successfully notified an alarm from {}".format(a.event_detector.timeline.board_id,
                        # a.event_detector.__class__.__name__))
                except:
                    self.logger.exception(
                        "board: {}, Notify alarm from {} got exception.".format(
                            target_alarm.event_detector.timeline.board_id,
                            target_alarm.event_detector.__class__.__name__))

    def processInfos(self):
        while True:
            target_info = None
            if self.infos and len(self.infos) > 0:
                target_info = self.infos.pop(0)
            if not target_info:
                time.sleep(1)
            elif target_info.code == "general_data":
                try:
                    upload_response = requests.post(EventAlarmWebServiceNotifier.URL_Gather,
                                                    headers=EventAlarmWebServiceNotifier.HEADERS,
                                                    data=None,
                                                    json=target_info.data)
                    if upload_response.status_code != 200 or upload_response.json()["code"] != 200:
                        self.logger.error("board: {}, liftId:{},Notify alarm from {} got error: {}".format(
                            target_info.event_detector.timeline.board_id, target_info.event_detector.timeline.liftId,
                            target_info.event_detector.__class__.__name__, upload_response.text[:500]))
                except:
                    self.logger.exception("board: {}, upload general_data from {} got exception.".format(
                        target_info.event_detector.timeline.board_id, target_info.event_detector.__class__.__name__))
            else:
                post_data = {"device_id": target_info.event_detector.timeline.board_id,
                             "warning_type": target_info.code,
                             "level": target_info.priority.value,
                             "description": target_info.description,
                             "original_timestamp": str(target_info.original_utc_timestamp),
                             "base64string": target_info.imageText,
                             "data": target_info.data}
                try:
                    post_response = requests.post(EventAlarmWebServiceNotifier.URL,
                                                  headers=EventAlarmWebServiceNotifier.HEADERS,
                                                  data=None,
                                                  json=post_data)
                    if post_response.status_code != 200:
                        self.logger.error("board: {}, Notify alarm from {} got error: {}".format(
                            target_info.event_detector.timeline.board_id,
                            target_info.event_detector.__class__.__name__,
                            post_response.text[:500]))
                    else:
                        pass
                except:
                    self.logger.exception("board: {}, upload doorstate change from {} got exception.".format(
                        target_info.event_detector.timeline.board_id, target_info.event_detector.__class__.__name__))