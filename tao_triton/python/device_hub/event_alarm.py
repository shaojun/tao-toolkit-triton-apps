import _thread
import datetime
import threading
import time
from enum import Enum
from typing import List
import tao_triton.python.device_hub.event_detector as event_detector
import requests


class EventAlarmPriority(Enum):
    VERBOSE = 9
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    FATAL = 4


class EventAlarm:
    def __init__(self, event_detector, original_utc_timestamp: datetime,
                 priority: EventAlarmPriority,
                 description: str, code="", data={}):
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
    HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, logging):
        self.logger = logging.getLogger("eventAlarmWebServiceNotifierLogger")
        self.alarms = []
        _thread.start_new_thread(self.processAlarms, ())

    def notify(self, alarms: List[EventAlarm]):
        if not alarms or len(alarms) == 0:
            return
        self.alarms.append(alarms)

    def processAlarms(self):
        while True:
            targert_alarms = None
            if self.alarms and len(self.alarms) > 0:
                targert_alarms = self.alarms.pop(0)
            if not targert_alarms:
                time.sleep(1)
            else:
                post_data = None
                target_alarm = targert_alarms[0]
                if target_alarm.event_detector.__class__.__name__ == event_detector.ElectricBicycleEnteringEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "007",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "data": target_alarm.data}
                elif target_alarm.event_detector.__class__.__name__ == event_detector.GasTankEnteringEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "0021",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "data": target_alarm.data}
                elif target_alarm.event_detector.__class__.__name__ == event_detector.DoorOpenedForLongtimeEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "008",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "data": target_alarm.data}
                elif target_alarm.event_detector.__class__.__name__ == event_detector.DoorRepeatlyOpenAndCloseEventDetector.__name__:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id, "warning_type": "004",
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
                                 "data": target_alarm.data}
                else:
                    post_data = {"device_id": target_alarm.event_detector.timeline.board_id,
                                 "warning_type": target_alarm.code,
                                 "level": target_alarm.priority.value,
                                 "description": target_alarm.description,
                                 "original_timestamp": str(target_alarm.original_utc_timestamp),
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