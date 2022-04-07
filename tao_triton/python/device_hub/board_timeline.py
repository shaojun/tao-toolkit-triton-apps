import datetime
import time
import os
from enum import Enum
from typing import List

import base64_tao_client

from kafka import KafkaConsumer
import json
import uuid


class TimelineItemType(Enum):
    # the item is sent by object detect app
    OBJECT_DETECT = 1
    # the item is sent by speed sensor
    SENSOR_READ_SPEED = 2
    SENSOR_READ_PRESSURE = 3
    SENSOR_READ_ACCELERATOR = 4
    SENSOR_READ_PEOPLE_DETECT = 5

    # the item is autoly generated from local, used for case of remote side muted(connection broken? remote app crash?)
    LOCAL_IDLE_LOOP = 99


class TimelineItem:
    def __init__(self, board_id: str, type: TimelineItemType, original_timestamp_str: str, board_msg_id: str,
                 raw_data: str):
        self.board_id = board_id
        self.type = type
        # the time from object detect app mostly use utc ZONE 0 time, like 2022-04-05T02:42:02.392Z
        self.original_timestamp_str = original_timestamp_str
        # to datetime type
        self.original_timestamp \
            = datetime.datetime.fromisoformat(original_timestamp_str.replace("Z", "+00:00"))
        # the server side time when received this msg
        self.local_utc_timestamp = datetime.datetime.fromisoformat(datetime.datetime.now(
            datetime.timezone.utc).astimezone().isoformat())
        self.local_timestamp = datetime.datetime.now()
        self.board_msg_id = board_msg_id
        self.raw_data = raw_data
        self.consumed = False


class BoardTimeline:
    import datetime
    # by seconds
    Timeline_Items_Max_Survive_Time = 120

    def __init__(self, logging, board_id: str, items: List[TimelineItem], event_detectors,
                 event_alarm_notifiers):
        cached_state_objects = []
        for d in event_detectors:
            cached_state_objects.append({"key": d.__class__.__name__, "value": None})
        self.cached_state_objects = cached_state_objects
        self.last_state_update_local_timestamp = None
        self.logger = logging.getLogger(__name__)
        self.board_id = board_id
        self.items = items
        self.event_detectors = event_detectors
        self.event_alarm_notifiers = event_alarm_notifiers

    def add_item(self, item: TimelineItem):
        # self.logger.debug("board: {} is adding TimelineItem...  type: {}".format(self.board_id, item.type.name))
        self.items.append(item)
        if len(self.items) % 5 == 0:
            self.__purge_items()
        event_alarms = []
        for d in self.event_detectors:
            t0 = time.time()
            new_alarms = None
            processing_items = None
            if d.get_timeline_item_filter():
                processing_items = d.get_timeline_item_filter()(self.items)
            elif self.items:
                processing_items = self.items
            else:
                continue
            state_obj_dict_per_detector = [so for so in self.cached_state_objects if so["key"] == d.__class__.__name__]
            if state_obj_dict_per_detector:
                wrapper_for_reference_pass_in = [state_obj_dict_per_detector[0]["value"], self.cached_state_objects]
                new_alarms = d.detect(wrapper_for_reference_pass_in, processing_items)
                # store back
                state_obj_dict_per_detector[0]["value"] = wrapper_for_reference_pass_in[0]
            else:
                new_alarms = d.detect(None, processing_items)

            if new_alarms:
                for a in new_alarms:
                    a.board_id = self.board_id
                    event_alarms.append(a)
            t1 = time.time()
            perf_time_used_by_ms = (t1 - t0) * 1000
            if perf_time_used_by_ms >= 800:
                self.logger.debug(
                    "event_detector: {} used time(ms): {}".format(d.__class__, perf_time_used_by_ms))

        if event_alarms:
            t0 = time.time()
            for n in self.event_alarm_notifiers:
                n.notify(event_alarms)
            t1 = time.time()
            perf_time_used_by_ms = (t1 - t0) * 1000
            if perf_time_used_by_ms >= 800:
                self.logger.debug(
                    "event_alarms total used time(ms): {}".format(perf_time_used_by_ms))

    def __purge_items(self):
        survived = [item for item in self.items if
                    (datetime.datetime.now(datetime.timezone.utc) - item.original_timestamp)
                        .total_seconds() <= BoardTimeline.Timeline_Items_Max_Survive_Time and not item.consumed]
        self.items = survived


class EventDetectorBase:
    def __init__(self, logging):
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return None

        return None

    def detect(self, board_timeline: BoardTimeline, filtered_timeline_items: List[TimelineItem]) -> []:
        return None

    # def where(self, items: List[TimelineItem], last_count: int):
    #     return items[-last_count:]


class EventAlarmPriority(Enum):
    VERBOSE = 1
    DEBUG = 2
    INFO = 3
    WARNING = 4
    ERROR = 5
    FATAL = 6


class EventAlarm:
    def __init__(self, priority: EventAlarmPriority, description: str):
        self.priority = priority
        self.description = description
        self.board_id = None

    def get_board_id(self):
        return self.board_id


class EventAlarmNotifierBase:
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        pass


class EventAlarmDummyNotifier:
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        if not alarms:
            return
        for a in alarms:
            self.logger.info(
                "Notifying alarm from board: {} with priority: {} -> {}"
                    .format(a.get_board_id(), a.priority, a.description))
