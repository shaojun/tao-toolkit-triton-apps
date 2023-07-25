from __future__ import annotations
import datetime
from json import dumps
import time
from enum import Enum
from typing import List

from kafka import KafkaProducer


class TimelineItemType(Enum):
    # the item is sent by object detect app
    OBJECT_DETECT = 1
    # the item is sent by speed sensor
    SENSOR_READ_SPEED = 2
    SENSOR_READ_PRESSURE = 3
    SENSOR_READ_ACCELERATOR = 4
    SENSOR_READ_PEOPLE_DETECT = 5
    SENSOR_READ_ELECTRIC_SWITCH = 6
    # the item is sent by watch dog
    UPDATE_RESULT = 7
    CAMERA_BLOCKED = 8
    CAMERA_VECHILE_EVENT = 9

    # the item is autoly generated from local, used for case of remote side muted(connection broken? remote app crash?)
    LOCAL_IDLE_LOOP = 99


class TimelineItem:
    def __init__(self, timeline: BoardTimeline, item_type: TimelineItemType, original_timestamp_str: str,
                 board_msg_id: str,
                 raw_data: str):
        """

        @type item_type: TimelineItemType
        @type timeline: BoardTimeline
        """
        self.timeline = timeline
        self.item_type = item_type
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
    # by seconds
    Timeline_Items_Max_Survive_Time = 120

    def __init__(self, logging, board_id: str, items: List[TimelineItem], event_detectors,
                 event_alarm_notifiers, producer):
        self.last_state_update_local_timestamp = None
        self.logger = logging.getLogger(__name__)
        self.board_id = board_id
        self.items = items
        self.event_detectors = event_detectors
        self.event_alarm_notifiers = event_alarm_notifiers
        # self.producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
        #                              value_serializer=lambda x: dumps(x).encode('utf-8'))
        self.producer = producer

        for d in event_detectors:
            d.prepare(self, event_detectors)

    def add_items(self, items: List[TimelineItem]):
        # self.logger.debug("board: {} is adding TimelineItem(s)...  type: {}".format(self.board_id, items[0].type.name))
        self.items += items
        if len(self.items) % 5 == 0:
            self.__purge_items()

        self.foward_object_to_board(items)
        # if len(items) == 1 and items[0].raw_data == '':
        #    return

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

            if processing_items:
                new_alarms = d.detect(processing_items)

            if new_alarms:
                for a in new_alarms:
                    a.timeline = self.board_id
                    event_alarms.append(a)
            t1 = time.time()
            perf_time_used_by_ms = (t1 - t0) * 1000
            if perf_time_used_by_ms >= 600:
                self.logger.info(
                    "board: {}, {} detect used time(ms): {}".format(self.board_id, d.__class__.__name__, perf_time_used_by_ms))

        if event_alarms:
            t0 = time.time()
            for n in self.event_alarm_notifiers:
                n.notify(event_alarms)
            t1 = time.time()
            perf_time_used_by_ms = (t1 - t0) * 1000
            if perf_time_used_by_ms >= 800:
                self.logger.info(
                    "event_alarms total used time(ms): {}".format(perf_time_used_by_ms))

    def __purge_items(self):
        survived = [item for item in self.items if
                    (datetime.datetime.now(datetime.timezone.utc) - item.original_timestamp)
                    .total_seconds() <= BoardTimeline.Timeline_Items_Max_Survive_Time and not item.consumed]
        self.items = survived

    def foward_object_to_board(self, items: List[TimelineItem]):
        if len(items) == 0:
            return
        if items[0].item_type != TimelineItemType.OBJECT_DETECT:
            return

        message = ""
        for item in items:
            if "Vehicle|#|TwoWheeler" in item.raw_data:
                message = "Vehicle|#|TwoWheeler"
        try:
            obj_info_list = []
            obj_info_list.append(message)
            self.producer.send(self.board_id + "_dh", {
                'version': '4.1',
                'id': 1913,
                '@timestamp': '{}'.format(datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                'sensorId': '{}'.format(self.board_id + "_dh"), 'objects': obj_info_list})
            # self.producer.flush(5)
            # producer.close()
        except:
            self.logger.exception(
                "send electric-bicycle confirmed message to kafka(...) rasised an exception:")