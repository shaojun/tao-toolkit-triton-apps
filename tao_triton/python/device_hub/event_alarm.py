import datetime
from enum import Enum
from typing import List
import event_detector
import requests


class EventAlarmPriority(Enum):
    VERBOSE = 9
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    FATAL = 4


class EventAlarm:
    def __init__(self, event_detector: event_detector.EventDetectorBase, original_utc_timestamp: datetime,
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
            self.logger.info(
                "board: {}, Notifying alarm(by {}) with priority: {} -> {}".format(
                    a.event_detector.timeline.board_id, 
                    a.event_detector.__class__.__name__, 
                    a.priority, a.description))


class EventAlarmWebServiceNotifier:
    URL = "http://49.235.97.14:8801/warning"
    HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        if not alarms:
            return
        for a in alarms:
            post_data = None
            if a.event_detector.__class__.__name__ == event_detector.ElectricBicycleEnteringEventDetector.__name__:
                post_data = {"device_id": a.event_detector.timeline.board_id, "warning_type": "007",
                             "level": a.priority.value,
                             "description": a.description,
                             "original_timestamp": str(a.original_utc_timestamp),
                             "data": a.data}
            elif a.event_detector.__class__.__name__ == event_detector.GasTankEnteringEventDetector.__name__:
                post_data = {"device_id": a.event_detector.timeline.board_id, "warning_type": "0021",
                             "level": a.priority.value,
                             "description": a.description,
                             "original_timestamp": str(a.original_utc_timestamp),
                             "data": a.data}
            elif a.event_detector.__class__.__name__ == event_detector.DoorOpenedForLongtimeEventDetector.__name__:
                post_data = {"device_id": a.event_detector.timeline.board_id, "warning_type": "008",
                             "level": a.priority.value,
                             "description": a.description,
                             "original_timestamp": str(a.original_utc_timestamp),
                             "data": a.data}
            elif a.event_detector.__class__.__name__ == event_detector.DoorRepeatlyOpenAndCloseEventDetector.__name__:
                post_data = {"device_id": a.event_detector.timeline.board_id, "warning_type": "004",
                             "level": a.priority.value,
                             "description": a.description,
                             "original_timestamp": str(a.original_utc_timestamp),
                             "data": a.data}
            else:
                post_data = {"device_id": a.event_detector.timeline.board_id, "warning_type": a.code,
                             "level": a.priority.value,
                             "description": a.description,
                             "original_timestamp": str(a.original_utc_timestamp),
                             "data": a.data}
            try:
                self.logger.info(
                "board: {}, Notifying alarm(by {}) with priority: {} -> {}".format(
                    a.event_detector.timeline.board_id, 
                    a.event_detector.__class__.__name__, 
                    a.priority, a.description))
                    
                # level: Debug=0, Info=1, Warning=2, Error=3, Fatal=4
                post_response = requests.post(EventAlarmWebServiceNotifier.URL,
                                              headers=EventAlarmWebServiceNotifier.HEADERS,
                                              data=None,
                                              json=post_data)
                if post_response.status_code != 200:
                    self.logger.error(
                        "board: {}, Notify alarm from {} got error: {}".format(
                            a.event_detector.timeline.board_id,
                            a.event_detector.__class__.__name__,
                            post_response.text[:500]))
                else:
                    pass
                    # self.logger.debug(
                    #     "board: {}, Successfully notified an alarm from {}".format(a.event_detector.timeline.board_id,
                    # a.event_detector.__class__.__name__))
            except:
                self.logger.exception(
                    "board: {}, Notify alarm from {} got exception.".format(
                        a.event_detector.timeline.board_id,
                        a.event_detector.__class__.__name__))
