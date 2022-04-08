import datetime
import re
import time
import os
import requests
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


class EventAlarmPriority(Enum):
    VERBOSE = 9
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    FATAL = 4


class EventAlarm:
    def __init__(self, event_detector, original_utc_timestamp: datetime, priority: EventAlarmPriority,
                 description: str):
        """

        @type event_detector: EventDetectorBase
        """
        self.event_detector = event_detector
        self.priority = priority
        self.description = description
        self.board_id = None

        # the alarm may be triggered at remote(board) side, this timestamp is from remote(board)
        self.original_utc_timestamp = original_utc_timestamp


class EventDetectorBase:
    def __init__(self, logging):
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return None

        return None

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None

    # def where(self, items: List[TimelineItem], last_count: int):
    #     return items[-last_count:]


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
                    .format(a.board_id, a.priority, a.description))


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
            if a.event_detector.__class__.__name__ == ElectricBicycleEnteringEventDetector.__name__:
                post_data = {"deviceId": a.board_id, "warningType": "007",
                             "level": a.priority.value,
                             "data": {"description": a.description,
                                      "original_timestamp": str(a.original_utc_timestamp)}}
            elif a.event_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                post_data = {"deviceId": a.board_id, "warningType": "007",
                             "level": a.priority.value,
                             "data": {"description": a.description,
                                      "original_timestamp": str(a.original_utc_timestamp)}}

            try:
                # level: Debug=0, Info=1, Warning=2, Error=3, Fatal=4
                post_response = requests.post(EventAlarmWebServiceNotifier.URL,
                                              headers=EventAlarmWebServiceNotifier.HEADERS,
                                              data=post_data)
                if post_response.status_code != 200:
                    self.logger.error(
                        "Notify alarm from {} got error: {}".format(a.event_detector.__class__.__name__,
                                                                    post_response.text[:500]))
                else:
                    self.logger.debug(
                        "Successfully notified an alarm from {}".format(a.event_detector.__class__.__name__))
            except:
                self.logger.exception(
                    "Notify alarm from {} got exception.".format(a.event_detector.__class__.__name__))


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

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        last_state_obj = None
        new_state_obj = None
        if state_obj_wrapper and state_obj_wrapper[0]:
            last_state_obj = state_obj_wrapper[0]
            last_door_state = last_state_obj["last_door_state"]
            last_state_timestamp = last_state_obj["last_state_timestamp"]
            if "extra_data" in last_state_obj:
                extra_data = last_state_obj["extra_data"]

        # show how to retrieve other detector's state obj
        if state_obj_wrapper and len(state_obj_wrapper) >= 2 and state_obj_wrapper[1]:
            target_state_obj_dicts = [state_obj_dict for state_obj_dict in state_obj_wrapper[1] if
                                      state_obj_dict[
                                          "key"] == ElectricBicycleEnteringEventDetector.__name__]
            if target_state_obj_dicts and target_state_obj_dicts[0]["value"]:
                ElectricBicycleEnteringEventDetector_state_obj = target_state_obj_dicts[0]["value"]
                if "last_infer_ebic_timestamp" in ElectricBicycleEnteringEventDetector_state_obj:
                    last_infer_ebic_timestamp = ElectricBicycleEnteringEventDetector_state_obj[
                        "last_infer_ebic_timestamp"]

        recent_items = [i for i in filtered_timeline_items if
                        (datetime.datetime.now() - i.local_timestamp).total_seconds() <= 3]
        for ri in reversed(recent_items):
            if ri.type == TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_door_state": "OPEN", "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "Vehicle|#|DoorWarningSign" in ri.raw_data:
                new_state_obj = {"last_door_state": "CLOSE", "last_state_timestamp": str(datetime.datetime.now())}
                break

        if not new_state_obj:
            new_state_obj = {"last_door_state": "OPEN", "last_state_timestamp": str(datetime.datetime.now())}

        # store it back, and it will be passed in at next call
        state_obj_wrapper[0] = new_state_obj
        if (last_state_obj and last_state_obj["last_door_state"] != new_state_obj[
            "last_door_state"]) or last_state_obj == None:
            return [
                EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                           EventAlarmPriority.INFO,
                           "Door state changed to: {}".format(new_state_obj["last_door_state"]))]
        return None


#
# 电动车检测告警（电动车入梯)
class ElectricBicycleEnteringEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    not i.consumed
                    and
                    # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                    (i.type == TimelineItemType.OBJECT_DETECT and "Vehicle|#|TwoWheeler" in i.raw_data)]

        return filter

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        last_state_obj = None
        new_state_obj = None
        if state_obj_wrapper and state_obj_wrapper[0]:
            last_state_obj = state_obj_wrapper[0]

        event_alarms = []
        for item in filtered_timeline_items:
            item.consumed = True
            if last_state_obj and "last_infer_ebic_timestamp" in last_state_obj:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - last_state_obj["last_infer_ebic_timestamp"]).total_seconds()
                if last_report_time_diff <= 10:
                    continue

            new_state_obj = {"last_infer_ebic_timestamp": datetime.datetime.now()}
            # store it back, and it will be passed in at next call
            state_obj_wrapper[0] = new_state_obj

            sections = item.raw_data.split('|')
            self.logger.debug(
                "E-bic is detected at board: {}, re-infer it from triton...".format(item.board_id))
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
            self.logger.debug("      (localConf:{})infer_results: {}".format(edge_board_confidence, infer_results))
            m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)', infer_results)
            if m and m.group(0):
                infer_server_confid = float(m.group(0))
                if infer_server_confid >= 0.5:
                    event_alarms.append(
                        EventAlarm(self, item.original_timestamp, EventAlarmPriority.ERROR,
                                   "detected electric-bicycle entering elevator with board confid: {}, server confid: {}".format(
                                       edge_board_confidence, infer_server_confid)))
                else:
                    self.logger.debug(
                        "      sink this detect due to infer server give low confidence: {}".format(m.group(0)))

        return event_alarms


#
# 遮挡门告警	判断电梯发生遮挡门事件
class BlockingDoorEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 困人	判断电梯发生困人事件
class PeopleStuckEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
        return None


#
# 超速	判断电梯发生超速故障
class ElevatorOverspeedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                         (i.type == TimelineItemType.SENSOR_READ_SPEED and "xxxxxx" in i.raw_data))]

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 反复开关门	判断电梯发生反复开关门故障
class ElevatorDoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 剧烈运动	判断轿厢乘客剧烈运动
class PassagerVigorousExerciseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 运行中开门	判断电梯发生运行中开门故障
class DoorOpeningAtMovingEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 急停	判断电梯发生急停故障
class ElevatorSuddenlyStoppedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 长时间开门	门开着超过一定时间
class DoorOpenedForLongtimeEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 无人高频运行	轿厢内没人, 上上下下跑来跑去
class ElevatorMovingWithoutPeopleInEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None
