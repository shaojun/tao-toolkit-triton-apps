import datetime
import time
import os
from typing import List

import base64_tao_client

from kafka import KafkaConsumer
import json
import uuid

from python.device_hub.timeline_event_alarm import EventAlarmNotifierBase
from python.device_hub.timeline_event_detectors import EventDetectorBase, TimelineItem


class BoardTimeline:
    import datetime
    # by seconds
    Timeline_Items_Max_Survive_Time = 120

    def __init__(self, logging, board_id: str, items: List[TimelineItem], event_detectors: List[EventDetectorBase],
                 event_alarm_notifiers: List[EventAlarmNotifierBase]):
        self.logger = logging.getLogger(__name__)
        self.board_id = board_id
        self.items = items
        self.event_detectors = event_detectors
        self.event_alarm_notifiers = event_alarm_notifiers

    def add_item(self, item: TimelineItem):
        self.items.append(item)
        if len(self.items) % 5 == 0:
            self.__purge_items()
        event_alarms = []
        for d in self.event_detectors:
            t0 = time.time()
            new_alarms = None
            if d.get_timeline_item_filter():
                filtered_items = d.get_timeline_item_filter()(self.items)
                if filtered_items:
                    new_alarms = d.detect(filtered_items)
            elif self.items:
                new_alarms = d.detect(self.items)
            else:
                continue
            if new_alarms:
                for a in new_alarms:
                    a.board_id = self.board_id
                    event_alarms.append(a)
            t1 = time.time()
            self.logger.debug(
                "event_detector: {} used time(ms): {}".format(d.__class__, (t1 - t0) * 1000))
        if event_alarms:
            t0 = time.time()
            for n in self.event_alarm_notifiers:
                n.notify(event_alarms)
            t1 = time.time()

            self.logger.debug(
                "event_alarms total used time(ms): {}".format((t1 - t0) * 1000))

    def __purge_items(self):
        survived = [item for item in self.items if
                    (datetime.datetime.now(datetime.timezone.utc)
                     - datetime.datetime.fromisoformat(item.original_timestamp_str.replace("Z", "+00:00")))
                        .total_seconds() <= BoardTimeline.Timeline_Items_Max_Survive_Time and not item.consumed]
        self.items = survived
