import logging
import os
import datetime
from typing import List

import yaml

from python.device_hub import base64_tao_client
from python.device_hub.timeline_event_alarm import EventAlarm, EventAlarmPriority


class TimelineItemType:
    OBJECT_DETECT = 1
    SENSOR_READ_SPEED = 2
    SENSOR_READ_PRESSURE = 3
    SENSOR_READ_ACCELERATOR = 4
    SENSOR_READ_PEOPLE_DETECT = 5


class TimelineItem:
    def __init__(self, type: TimelineItemType, original_timestamp: str, board_msg_id: str, raw_data: str):
        self.type = type
        self.original_timestamp = original_timestamp
        self.board_msg_id = board_msg_id
        self.raw_data = raw_data
        self.consumed = False


class EventDetectorBase:
    def __init__(self, logging):
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return None

        return None

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 门的基本状态改变检测： 已开门， 已关门
class DoorStateChangedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    # def get_timeline_item_filter(self):
    #     def filter(timeline_items: List[TimelineItem]):
    #         return [i for i in timeline_items if
    #                 i.type == TimelineItemType.OBJECT_DETECT and
    #                 not i.consumed and "Vehicle|#|DoorWarningSign" in i.raw_data]
    #
    #     return filter

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        if "Vehicle|#|DoorWarningSign" in filtered_timeline_items[-1].raw_data:
            return EventAlarm(EventAlarmPriority.INFO, "Door is in CLOSE state")
        else:
            return EventAlarm(EventAlarmPriority.INFO, "Door is in OPEN state")


#
# 遮挡门告警	判断电梯发生遮挡门事件
class BlockingDoorEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 困人	判断电梯发生困人事件
class PeopleStuckEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
        return None


#
# 电动车检测告警（电动车入梯)
class ElectricBicycleEnteringEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.OBJECT_DETECT and
                    not i.consumed and "Vehicle|#|TwoWheeler" in i.raw_data]

        return filter

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        minimal_handle_interval_by_second = 10
        last_handled_original_datetime_in_msg = datetime.datetime.fromisoformat(
            '1999-02-07T05:01:06.354Z'.replace("Z", "+00:00"))
        for item in filtered_timeline_items:
            item.consumed = True
            current_original_datetime_in_msg = datetime.datetime.fromisoformat(
                item.original_timestamp.replace("Z", "+00:00"))
            if (current_original_datetime_in_msg - last_handled_original_datetime_in_msg).total_seconds() \
                    <= minimal_handle_interval_by_second:
                continue
            last_handled_original_datetime_in_msg = datetime.datetime.fromisoformat(
                item.original_timestamp.replace("Z", "+00:00"))
            sections = item.raw_data.split('|')
            self.logger.info(
                "A suspect electric-bicycle is detected, will send to infer server to further make sure of it...")
            # the last but one is the detected object image file with base64 encoded text,
            # and the section is prefixed with-> base64_image_data:
            cropped_base64_image_file_text = sections[len(sections) - 2][len("base64_image_data:"):]
            edge_board_confidence = sections[len(sections) - 1]
            # infer_results = base64_tao_client.infer(FLAGS.verbose, FLAGS.async_set, FLAGS.streaming,
            #                                         FLAGS.model_name, FLAGS.model_version,
            #                                         FLAGS.batch_size, FLAGS.class_list,
            #                                         False, FLAGS.url, FLAGS.protocol, FLAGS.mode,
            #                                         FLAGS.output_path,
            #                                         [cropped_base64_image_file_text])
            infer_results = base64_tao_client.infer(False, False, False,
                                                    "bicycletypenet_tao", "",
                                                    1, "bicycle,electric_bicycle",
                                                    False, "36.153.41.18:18000", "HTTP", "Classification",
                                                    os.path.join(os.getcwd(), "outputs"),
                                                    [cropped_base64_image_file_text])
            # sample: (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(0)=bicycle, 0.4476(1)=electric_bicycle
            # the `0.4476(1)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
            # which is less than 50%, so it's a bicycle, should not trigger alarm.
            self.logger.info("(localConf:{})infer_results: {}".format(edge_board_confidence, infer_results))

            return EventAlarm(EventAlarmPriority.ERROR, "detected a case of electric-bicycle entering elevator")

            # there're may have several electric_bicycle detected in single msg, for lower the cost of infer, here only detecting the first one.


#
# 超速	判断电梯发生超速故障
class ElevatorOverspeedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 反复开关门	判断电梯发生反复开关门故障
class ElevatorDoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 剧烈运动	判断轿厢乘客剧烈运动
class PassagerVigorousExerciseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 运行中开门	判断电梯发生运行中开门故障
class DoorOpeningAtMovingEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 急停	判断电梯发生急停故障
class ElevatorSuddenlyStoppedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 长时间开门	门开着超过一定时间
class DoorOpenedForLongtimeEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None


#
# 无人高频运行	轿厢内没人, 上上下下跑来跑去
class ElevatorMovingWithoutPeopleInEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    i.type == TimelineItemType.SENSOR_READ_SPEED and
                    not i.consumed and "xxxxxx" in i.raw_data]

    def detect(self, filtered_timeline_items: List[TimelineItem]):
        return None