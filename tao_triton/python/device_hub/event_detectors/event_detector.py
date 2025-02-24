import datetime
import os
import re
import time
from typing import List
import shutil
# from tao_triton.python.device_hub import base64_tao_client
# from tao_triton.python.device_hub.board_timeline import TimelineItem, TimelineItemType
from kafka import KafkaProducer
from PIL import Image
import base64
import io
import event_alarm
import board_timeline
from tao_triton.python.device_hub import util
from tao_triton.python.entrypoints import tao_client
from json import dumps
import uuid
import requests
from tao_triton.python.device_hub.event_detectors.event_detector_base import EventDetectorBase
from tao_triton.python.device_hub.event_detectors.electric_bicycle_entering_event_detector import \
    ElectricBicycleEnteringEventDetector
import paho.mqtt.client as paho_mqtt_client
import json
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer
from logging import Logger


class DoorStateSessionDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger("doorStateSessionLogger")
        self.is_session_detector = True

    def prepare(self, timeline, event_detectors):
        self.timeline = timeline

    # 对于判断session的detector, 传入的items为本轮的新upload项,任何路径都不需要上传报警
    def detect(self, filtered_timeline_items):
        object_items = [i for i in filtered_timeline_items if
                        not i.consumed and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        # 没有目标识别的物体, 如果门标处于某个状态超过60s将门标状态置为unkown
        if not (len(object_items) > 0):
            if self.timeline.door_state_session["door_state"] != "unkown" and self.timeline.door_state_session[
                "latest_current_state_item_time"] != None and \
                    (datetime.datetime.now(datetime.timezone.utc) - self.timeline.door_state_session[
                        "latest_current_state_item_time"]).total_seconds() > 60:
                self.timeline.door_state_session["door_state"] = "unkown"
                self.timeline.door_state_session["state_start_at"] = None
                self.timeline.door_state_session["latest_current_state_item_time"] = None
                self.timeline.door_state_session["is_frame_grayscale"] = False
                self.timeline.door_state_session["latest_grayscal_time"] = None
                self.timeline.door_state_session["door_closed_no_doorsign_count"] = 0
        else:
            is_frame_grayscale = object_items[0].is_frame_grayscale
            # 有识别到物体，灰度模式下并且现有状态有超一分钟未更新过了 将门标状态置为unkown状态
            # 非灰度模式依赖门标判断门的状态
            door_sign_items = [i for i in object_items if
                               not i.consumed and "Vehicle|#|DoorWarningSign" in i.raw_data]
            if is_frame_grayscale:
                if self.timeline.door_state_session["door_state"] != "unkown" and \
                        self.timeline.door_state_session["latest_current_state_item_time"] != None and \
                        (datetime.datetime.now(datetime.timezone.utc) - self.timeline.door_state_session[
                            "latest_current_state_item_time"]).total_seconds() > 60:
                    self.timeline.door_state_session["door_state"] = "unkown"
                    self.timeline.door_state_session["session_start_at"] = None
                    self.timeline.door_state_session["latest_current_state_item_time"] = None
                    self.timeline.door_state_session["door_closed_no_doorsign_count"] = 0
                    self.logger.debug("board:{} set door state to unkown due to the grey frame".format(
                        self.timeline.board_id))
                self.timeline.door_state_session["is_frame_grayscale"] = True
                self.timeline.door_state_session["latest_grayscal_time"] = datetime.datetime.now(datetime.timezone.utc)
            else:
                self.timeline.door_state_session["is_frame_grayscale"] = False
                # 有门标
                if len(door_sign_items) > 0:
                    if self.timeline.door_state_session["door_state"] != "closed":
                        self.timeline.door_state_session["door_state"] = "closed"
                        self.timeline.door_state_session["door_closed_no_doorsign_count"] = 0
                        self.timeline.door_state_session["session_start_at"] = door_sign_items[0].original_timestamp
                        self.timeline.door_state_session["latest_current_state_item_time"] = door_sign_items[
                            0].original_timestamp
                        self.logger.debug(
                            "board:{} change door state to closed, door sign received".format(self.timeline.board_id))
                    else:
                        self.timeline.door_state_session["latest_current_state_item_time"] = door_sign_items[
                            0].original_timestamp
                else:
                    # 没有门标, 如果门处于关闭状态，则门标消失3次及以上时认为门开
                    if self.timeline.door_state_session["door_state"] != "open":
                        if self.timeline.door_state_session["door_state"] == "closed" and \
                                self.timeline.door_state_session["door_closed_no_doorsign_count"] > 2:
                            self.timeline.door_state_session["door_state"] = "open"
                            self.timeline.door_state_session["session_start_at"] = datetime.datetime.now(
                                datetime.timezone.utc)
                            self.timeline.door_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                                datetime.timezone.utc)
                            self.logger.debug(
                                "board:{} change door state to open, the count for no door warning sign:{}".format(
                                    self.timeline.board_id,
                                    self.timeline.door_state_session["door_closed_no_doorsign_count"]))
                            self.timeline.door_state_session["door_closed_no_doorsign_count"] = 0
                        elif self.timeline.door_state_session["door_state"] == "closed":
                            self.timeline.door_state_session["door_closed_no_doorsign_count"] += 1
                        else:
                            self.timeline.door_state_session["door_state"] = "open"
                            self.timeline.door_state_session["session_start_at"] = datetime.datetime.now(
                                datetime.timezone.utc)
                            self.timeline.door_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                                datetime.timezone.utc)
                            self.timeline.door_state_session["door_closed_no_doorsign_count"] = 0
                            self.logger.debug(
                                "board:{} change door state to open, previous is unkown".format(self.timeline.board_id))
                    else:
                        # 门已经处于开的状态，只更新最近一次保持门状态的时间
                        self.timeline.door_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                            datetime.timezone.utc)
        return None


class RunningStateSessionDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger("runningStateSessionLogger")
        self.is_session_detector = True

    def prepare(self, timeline, event_detectors):
        self.timeline = timeline

    # 对于判断session的detector, 传入的items为本轮的新upload项,任何路径都不需要上传报警
    def detect(self, filtered_timeline_items):
        object_items = [i for i in filtered_timeline_items if
                        not i.consumed and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        # 上一次更新数据已超过6秒，设置电梯为静止状态
        if (datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "latest_current_state_item_time"]).total_seconds() > 6:
            self.timeline.lift_running_state_session["is_running"] = False
            self.timeline.lift_running_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                datetime.timezone.utc)
            self.timeline.lift_running_state_session["item_data"] = 0
            self.timeline.lift_running_state_session["session_start_at"] = datetime.datetime.now(datetime.timezone.utc)
        # 如果有速度上传
        if len(object_items) > 0:
            if self.timeline.lift_running_state_session["is_running"]:
                if abs(object_items[0].raw_data["speed"]) >= 0.1 or abs(self.timeline.lift_running_state_session[
                                                                            "item_data"]) > 0.1:
                    self.timeline.lift_running_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                        datetime.timezone.utc)
                    self.timeline.lift_running_state_session["item_data"] = object_items[0].raw_data["speed"]
                elif abs(self.timeline.lift_running_state_session["item_data"]) < 0.1:
                    self.timeline.lift_running_state_session["is_running"] = False
                    self.timeline.lift_running_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                        datetime.timezone.utc)
                    self.timeline.lift_running_state_session["item_data"] = object_items[0].raw_data["speed"]
                    self.timeline.lift_running_state_session["session_start_at"] = datetime.datetime.now(
                        datetime.timezone.utc)
                    self.logger.debug("board:{} close running session, current speed is:{}".format(
                        self.timeline.board_id, object_items[0].raw_data["speed"]))
            else:
                # 静止情况下，如果此次速度>0.1上次的速度也大于0.1则认为从静止变为运动
                if abs(object_items[0].raw_data["speed"]) >= 0.1 and abs(self.timeline.lift_running_state_session[
                                                                             "item_data"]) > 0.1:
                    self.timeline.lift_running_state_session["is_running"] = True
                    self.timeline.lift_running_state_session["session_start_at"] = datetime.datetime.now(
                        datetime.timezone.utc)
                    self.timeline.lift_running_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                        datetime.timezone.utc)
                    self.timeline.lift_running_state_session["item_data"] = object_items[0].raw_data["speed"]
                    self.logger.debug("board:{} running session start, current speed is:{}".format(
                        self.timeline.board_id, object_items[0].raw_data["speed"]))
                else:
                    # 如果此处跟上一次或这次的速度< 0.1则认为还是处于静止状态
                    self.timeline.lift_running_state_session["latest_current_state_item_time"] = datetime.datetime.now(
                        datetime.timezone.utc)
                    self.timeline.lift_running_state_session["item_data"] = object_items[0].raw_data["speed"]
        return None


#
# 门的基本状态改变检测： 已开门， 已关门
class DoorStateChangedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger(
            "doorStateChangedEventDetectorLogger")
        self.electricBicycleEnteringEventDetector_instance = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        for det in event_detectors:
            if det.__class__.__name__ == ElectricBicycleEnteringEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                self.electricBicycleEnteringEventDetector_instance = det
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == ElectricBicycleEnteringEventDetector.__name__:
            # handle event from ElectricBicycleEnteringEventDetector
            pass

    # def get_timeline_item_filter(self):
    #     def filter(timeline_items: List[TimelineItem]):
    #         return [i for i in timeline_items if
    #                 i.type == TimelineItemType.OBJECT_DETECT and
    #                 not i.consumed and "Vehicle|#|DoorWarningSign" in i.raw_data]
    #
    #     return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None

        # sample for show how to retrieve other detector's state obj
        if self.electricBicycleEnteringEventDetector_instance is not None:
            ebike_detector_state_obj = self.electricBicycleEnteringEventDetector_instance.state_obj
            if ebike_detector_state_obj is not None and "last_infer_ebic_timestamp" in ebike_detector_state_obj:
                last_infer_ebic_timestamp = ebike_detector_state_obj[
                    "last_infer_ebic_timestamp"]

        recent_items = [i for i in filtered_timeline_items if
                        not i.consumed and
                        i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                        (datetime.datetime.now() - i.local_timestamp).total_seconds() < 3]

        target_msg_original_timestamp_str = "" if len(
            recent_items) == 0 else recent_items[-1].original_timestamp_str

        target_items = []
        count = 0
        for item in reversed(recent_items):
            if item.original_timestamp_str != target_msg_original_timestamp_str:
                target_msg_original_timestamp_str = item.original_timestamp_str
                count = count + 1
            if count < 3:
                target_items.append(item)

        # target_items_last = [i for i in filtered_timeline_items if
        #                     i.original_timestamp_str == target_msg_original_timestamp_str]

        # target_items = recent_items
        # self.logger.debug(
        #    "total length of recent items:{}, target items:{}".format(len(recent_items), len(target_items)))
        """
        for item in target_items:
            if "Vehicle|#|TwoWheeler" in item.raw_data or "|TwoWheeler|confirmed" in item.raw_data:
                self.logger.debug("Item in door state detect. e-bike. original time:{}".format(
                    item.original_timestamp_str))
            elif "Vehicle|#|gastank" in item.raw_data:
                self.logger.debug("Item in door state detect. gastank. original time:{}".format(
                    item.original_timestamp_str))
            else:
                self.logger.debug(
                    "item in door state detect,board:{},board_msg_id:{}, item type:{},raw_data:{},original time:{}".format(
                        item.timeline.board_id,
                        item.board_msg_id,
                        item.item_type,
                        item.raw_data,
                        item.original_timestamp_str))
        """
        '''
        if len(target_items) > 0:
            log_item = target_items[0]
            self.logger.debug(
                "item in door state detect,board:{},board_msg_id:{}, item type:{},raw_data:{},original time:{}".format(
                    log_item.timeline.board_id,
                    log_item.board_msg_id,
                    log_item.item_type,
                    log_item.raw_data,
                    log_item.original_timestamp_str))
        '''
        hasPereson = "Y" if self.timeline.person_session["person_in"] else "N"
        for ri in target_items:
            if ri.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_door_state": "OPEN",
                                 "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "Vehicle|#|DoorWarningSign" in ri.raw_data:
                new_state_obj = {"last_door_state": "CLOSE",
                                 "last_state_timestamp": str(datetime.datetime.now())}
                break
        if not new_state_obj:
            new_state_obj = {"last_door_state": "OPEN",
                             "last_state_timestamp": str(datetime.datetime.now())}

        # store it back
        self.state_obj = new_state_obj
        if (last_state_obj and last_state_obj["last_door_state"] != new_state_obj[
            "last_door_state"]) or last_state_obj is None:
            self.__fire_on_property_changed_event_to_subscribers__(
                "door_state",
                {"last_state": "None" if last_state_obj is None else last_state_obj["last_door_state"],
                 "new_state": new_state_obj["last_door_state"],
                 "hasPerson": hasPereson})
            code = "DOOROPEN" if new_state_obj["last_door_state"] == "OPEN" else "DOORCLOSE"
            '''
            self.logger.debug("item in door state detect,board:{}, door state changed, new state is:{}".format(
                self.timeline.board_id,
                new_state_obj["last_door_state"]
            ))
            '''
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.INFO,
                                       "Door state changed to: {},human_inside:{}".format(
                                           new_state_obj["last_door_state"], hasPereson), code,
                                       {"human_inside": hasPereson})]
        return None


#
# 遮挡门告警	判断电梯发生遮挡门事件 1.电梯内有人 2.门处于打开状态 3.电梯静止
# 将长时间开门告警结合在此处，如果没人，门打开超过一定时间则报长时间开门
class BlockingDoorEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger("blockingDoorEventDetector")

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}
        self.door_long_time_open_history = []
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，如果它是处于关闭状态，那么就不用判断遮挡门
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now()}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            # and "Person|#" in i.raw_data
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT or
                         (
                                 i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data)) and
                    (datetime.datetime.now() - i.local_timestamp).total_seconds() < 120]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        door_state = None
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        last_state_object = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_object = self.state_obj["last_notify_timestamp"]
        # 提交告警结束
        if last_state_object and self.state_obj["alarm_code"] == "002" and self.canCloseTheAlarm(
                filtered_timeline_items):
            self.state_obj["last_notify_timestamp"] = None
            self.state_obj["alarm_code"] = ""
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "002")]
        elif last_state_object and self.state_obj["alarm_code"] == "008" and self.canCloseLongTimeOpen():
            self.state_obj["last_notify_timestamp"] = None
            self.state_obj["alarm_code"] = ""

            if "giveup" in self.state_obj and self.state_obj["giveup"]:
                self.state_obj["giveup"] = False
                return None

            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "008")]

        config_item_1 = [i for i in self.timeline.configures if i["code"] == "zdm"]
        config_time_1 = 90 if len(config_item_1) == 0 else int(config_item_1[0]["value"])

        # 是否符合长时间开门
        config_item = [i for i in self.timeline.configures if i["code"] == "csjkm"]
        config_time = 120 if len(config_item) == 0 else int(config_item[0]["value"])

        # 门为非打开状态，不需要判断长时间开门跟遮挡门
        if self.timeline.door_state_session["door_state"] != "open" or \
                self.timeline.door_state_session["session_start_at"] == None:
            return None

        object_items = [i for i in filtered_timeline_items if
                        i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        if len(object_items) == 0:
            return None

        # 如果已经产生过告警且没有结束，那么就不再处理
        if last_state_object:
            return None

        door_open_time_diff = (datetime.datetime.now(datetime.timezone.utc) - self.timeline.door_state_session[
            "session_start_at"]).total_seconds()

        #
        '''
        if not door_state or door_state["new_state"] != "OPEN":
            return None

        door_open_time_diff = (datetime.datetime.now(
        ) - door_state["notify_time"]).total_seconds()


        # 如果收到的开门状态时间还很短，那么不作遮挡判断
        if abs(door_open_time_diff) < config_time_1:
            return None
        '''

        can_raise_door_openned_long_time = False if abs(door_open_time_diff) < config_time else True

        can_raise_door_blocked = False if abs(door_open_time_diff) < config_time_1 else True
        # 电梯内没有人 不判断遮挡
        if not self.timeline.person_session["person_in"] or \
                (datetime.datetime.now(datetime.timezone.utc) - self.timeline.person_session[
                    "session_start_at"]).total_seconds() < 10:
            can_raise_door_blocked = False

        '''
        last_state_object = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_object = self.state_obj["last_notify_timestamp"]
        if last_state_object:
            notify_time_diff = (datetime.datetime.now() -
                                last_state_object).total_seconds()
            if notify_time_diff < 90:
                return None


        speed_timeline_items = [i for i in filtered_timeline_items if
                                i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                                "speed" in i.raw_data]
        # 未能获取到电梯速度，无法判断电梯是否静止
        if not len(speed_timeline_items) > 2:
            can_raise_door_blocked = False
            can_raise_door_openned_long_time = False
            # return None

        # new_state_object = None

        if len(speed_timeline_items) > 2:
            latest_speed_item = speed_timeline_items[-1]
            latest_speed_item_time_diff = (datetime.datetime.now(
                datetime.timezone.utc) - latest_speed_item.original_timestamp).total_seconds()
            third_speed_item = speed_timeline_items[-3]
            third_speed_item_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                          third_speed_item.original_timestamp).total_seconds()
            if abs(latest_speed_item_time_diff) < 4 and latest_speed_item.raw_data["speed"] < 0.1 and \
                    abs(third_speed_item_time_diff) < 6 and third_speed_item.raw_data["speed"] < 0.2:
                new_state_object = datetime.datetime.now()
            else:
                can_raise_door_blocked = False
                can_raise_door_openned_long_time = False
        '''
        if self.timeline.lift_running_state_session["is_running"] or (
                self.timeline.lift_running_state_session["is_running"] == False and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "session_start_at"]).total_seconds() < config_time_1):
            can_raise_door_blocked = False
            can_raise_door_openned_long_time = False

        if can_raise_door_blocked:
            self.logger.debug("board:{},遮挡门告警中，开门时长为{}s".format(self.timeline.board_id, door_open_time_diff))
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            self.state_obj["alarm_code"] = "002"
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "电梯门被遮挡", "002")]
        elif can_raise_door_openned_long_time:
            # session_start_at_time_str = self.timeline.door_state_session["session_start_at"].astimezone(datetime.timezone(datetime.timedelta(hours=8))).strftime(
            #                                          "%d/%m/%Y %H:%M:%S")
            alarms = [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                             event_alarm.EventAlarmPriority.ERROR,
                                             "长时间处于开门状态 - 门开着超过一定时间的故障, 最近一次门状态转变为'开'的UTC时间为: {}, 且持续时间 >= {}秒".format(
                                                 self.timeline.door_state_session["session_start_at"].strftime(
                                                     "%d/%m/%Y %H:%M:%S"),
                                                 door_open_time_diff),
                                             "008")]
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            self.state_obj["alarm_code"] = "008"

            self.state_obj["giveup"] = False
            if self.giveUpTheLongTimeOpen(datetime.datetime.now()):
                self.state_obj["giveup"] = True
                return None

            return alarms
        return None

    # 遮挡门告警发生后，电梯开始运行或关门状态持续30S
    def canCloseTheAlarm(self, filtered_timeline_items):
        '''
        speed_items = [i for i in filtered_timeline_items if
                       i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                       "speed" in i.raw_data and (datetime.datetime.now(datetime.timezone.utc) -
                                                  i.original_timestamp).total_seconds() < 6]
        if len(speed_items) > 2 and speed_items[0].raw_data["speed"] > 0.3 and \
                speed_items[-1].raw_data["speed"] > 0.3:
            return True
        '''
        # 电梯运行
        if self.timeline.lift_running_state_session["is_running"]:
            return True

        # 没人
        if not self.timeline.person_session["person_in"]:
            return True
        '''
        if self.state_obj and "door_state" in self.state_obj and \
                self.state_obj["door_state"]["new_state"] == "CLOSE" and \
                (datetime.datetime.now() - self.state_obj["door_state"]["notify_time"]).total_seconds() > 5:
            return True
        '''
        # 门状态不为开
        if self.timeline.door_state_session["door_state"] != "open":
            True

        return False

    # 是否可以关闭长时：关门解除
    def canCloseLongTimeOpen(self):
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]
        '''
        if last_state_obj and (datetime.datetime.now() - last_state_obj).total_seconds() > 10 and \
                self.state_obj and "door_state" in self.state_obj and \
                self.state_obj["door_state"]["new_state"] == "CLOSE":
            return True
        '''
        if last_state_obj and (datetime.datetime.now() - last_state_obj).total_seconds() > 10 and \
                self.timeline.door_state_session["door_state"] != "open":
            return True
        return False

    def giveUpTheLongTimeOpen(self, alarm_time):
        result = False
        if alarm_time.second % 9 != 0:
            result = True
        return result
        current_hour = datetime.datetime.now(datetime.timezone.utc).hour
        survived_alarms = [a for a in self.door_long_time_open_history if
                           (datetime.datetime.now() - a["time"]).total_seconds() > 10 * 60 * 60]
        self.door_long_time_open_history = survived_alarms
        target_alarms = [a for a in self.door_long_time_open_history if a["give_up"] == True]
        # 如果没有上报过长时间开门则上报
        if len(self.door_long_time_open_history) == 0:
            result = False
            self.door_long_time_open_history.append({"time": alarm_time, "give_up": False})
        # 20:00-9:00 utc tiem 12:00 16:00  01:00
        elif current_hour > 12 or current_hour < 1:
            if len(target_alarms) < 9:
                self.door_long_time_open_history.append({"time": alarm_time, "give_up": True})
                result = True
            else:
                self.door_long_time_open_history.append({"time": alarm_time, "give_up": False})
        else:
            if len(target_alarms) < 6:
                self.door_long_time_open_history.append({"time": alarm_time, "give_up": True})
                result = True
            else:
                self.door_long_time_open_history.append({"time": alarm_time, "give_up": False})
        if len(self.door_long_time_open_history) > 10:
            self.door_long_time_open_history.pop(0)
        return result


#
# 困人	判断电梯发生困人事件
class PeopleStuckEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger("peopleStuckEventDetector")
        self.state_obj = {"last_notify_timestamp": None}
        self.last_report_close_time = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "has_person": data["hasPerson"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and
                    ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and (
                            "Person|#" in i.raw_data in i.raw_data or "#|DoorWarningSign" in i.raw_data)) or
                     (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED))]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # self.logger.debug("board:{}, entering stuck detector".format(self.timeline.board_id))

        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        if last_state_obj and ((datetime.datetime.now() - last_state_obj).total_seconds() > 2 * 600 or
                               self.canCloseAlarm(filtered_timeline_items)):
            self.state_obj["last_notify_timestamp"] = None
            self.last_report_close_time = datetime.datetime.now()
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "001")]

        if last_state_obj != None:
            return None

        if self.last_report_close_time and (datetime.datetime.now() - self.last_report_close_time).total_seconds() < 60:
            return None

        '''
        door_state = None
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        # 关门的时候没有人，那么不进行困人判断
        if not door_state or door_state["new_state"] == "OPEN" or door_state["has_person"] == "N":
            return None
        '''

        if self.timeline.door_state_session["door_state"] != "closed":
            # self.logger.debug("board:{}, skip stuck detect door is open".format(self.timeline.board_id))
            return None

        kunren_sj_item = [i for i in self.timeline.configures if i["code"] == "krsj"]
        kunren_sj = 90 if len(kunren_sj_item) == 0 else int(kunren_sj_item[0]["value"])

        # 如果门关上还未超出2分钟则不认为困人
        if self.timeline.door_state_session["door_state"] == "closed" and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.door_state_session[
            "session_start_at"]).total_seconds() < kunren_sj:
            # self.logger.debug("board:{}, skip stuck detect door closed at:{}".format(self.timeline.board_id,
            #                                                                         self.timeline.door_state_session["session_start_at"].strftime("%d/%m/%Y %H:%M:%S")))
            return None

        new_state_obj = None

        # 电梯内没人
        if not self.timeline.person_session["person_in"] or self.timeline.person_session["session_start_at"] == None:
            # self.logger.debug("board:{}, skip stuck detect person session closed".format(self.timeline.board_id))
            return None

        # 人在电梯内的时间小于配置的困人时间
        if (datetime.datetime.now(datetime.timezone.utc) - self.timeline.person_session[
            "session_start_at"]).total_seconds() < kunren_sj:
            # self.logger.debug("board:{}, skip stuck detect person session start at:{}".format(self.timeline.board_id,
            #                                                                         self.timeline.person_session["session_start_at"].strftime("%d/%m/%Y %H:%M:%S")))
            return None
        # speed
        '''
        speed_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                         board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if len(speed_filtered_timeline_items) == 0:
            return None

        for item in speed_filtered_timeline_items:
            time_diff = (datetime.datetime.now(
                datetime.timezone.utc) - item.original_timestamp).total_seconds()
            # 如果120秒内电梯有在运动那么不认为困人
            if time_diff <= kunren_sj and abs(item.raw_data["speed"]) > 0.3:
                is_quiescent = False
        '''
        is_quiescent = False
        if self.timeline.lift_running_state_session["is_running"] == False and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "session_start_at"]).total_seconds() > kunren_sj:
            is_quiescent = True

        if is_quiescent:
            new_state_obj = {"people_stuck": "stuck",
                             "last_report_timestamp": datetime.datetime.now()}

        if new_state_obj:
            self.last_report_close_time = None
            self.logger.debug("board:{},raise alarm person session start at:{}".format(self.timeline.board_id,
                                                                                       self.timeline.person_session[
                                                                                           "session_start_at"].strftime(
                                                                                           "%d/%m/%Y %H:%M:%S")))
            # store it back, and it will be passed in at next call
            self.state_obj["last_notify_timestamp"] = new_state_obj["last_report_timestamp"];
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "{}发现有人困在电梯内，乘客进入UTC时间为{}, 梯门关闭UTC时间：{}".format(
                                           new_state_obj["last_report_timestamp"].strftime("%d/%m/%Y %H:%M:%S"),
                                           self.timeline.person_session["session_start_at"].strftime(
                                               "%d/%m/%Y %H:%M:%S"),
                                           self.timeline.door_state_session["session_start_at"].strftime(
                                               "%d/%m/%Y %H:%M:%S")),
                                       "001")]
        return None

    # 门开 或 轿厢内没人
    def canCloseAlarm(self, filtered_timeline_items):
        # speed > 0
        if self.timeline.lift_running_state_session["is_running"] and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "session_start_at"]).total_seconds() > 2:
            self.logger.debug("board:{} close the alarm due to lift is running")
            return True

        # person session 因人的不稳定性所以暂将结束告警条件中的人去掉
        '''
        if not self.timeline.person_session["person_in"]:
            self.logger.debug("board:{} close the alarm due to there's no person")
            return True
        '''

        # door state
        if self.timeline.door_state_session["door_state"] == "open":
            self.logger.debug("board:{} close the alarm due to door is open")
            return True

        return False
        '''
        door_state = None
        # self.logger.debug("board:{} Check if can close the alarm which uploaded at:{}".
        #                  format(self.timeline.board_id, str(self.state_obj["last_notify_timestamp"])))
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        # 门开
        if not door_state or door_state["new_state"] == "OPEN":
            return True

        filtered_person_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in
                                          i.raw_data
                                          and (door_state["notify_time"] - i.original_timestamp).total_seconds() < 0]
        # 电梯内没人
        if len(filtered_person_timeline_items) < 20:
            return True

        object_person = None
        object_person = filtered_person_timeline_items[-1]
        if (datetime.datetime.now(datetime.timezone.utc) - object_person.original_timestamp).total_seconds() > 10:
            return True

        # speed
        speed_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                         board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if len(speed_filtered_timeline_items) > 0:
            is_quiescent = True
            for item in speed_filtered_timeline_items:
                time_diff = (datetime.datetime.now(
                    datetime.timezone.utc) - item.original_timestamp).total_seconds()
                # 如果120秒内电梯有在运动那么不认为困人
                if time_diff <= 120 and abs(item.raw_data["speed"]) > 0.3:
                    is_quiescent = False
            if not is_quiescent:
                return True
        self.logger.debug("board:{} can not close the alarm which uploaded at:{}".format(self.timeline.board_id, str(
            self.state_obj["last_notify_timestamp"])))
        return False
        '''


#
# 超速	判断电梯发生超速故障
class ElevatorOverspeedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_speed_state": "normal", "last_speed": 0, "last_notify_time_stamp": None}
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data and
                         (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 10)]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        if len(filtered_timeline_items) < 6:
            return None

        if self.state_obj and "last_notify_time_stamp" in self.state_obj and self.state_obj["last_notify_time_stamp"]:
            time_diff = (datetime.datetime.now() - self.state_obj["last_notify_time_stamp"]).total_seconds()
            if self.canCloseAlarm(filtered_timeline_items, time_diff):
                self.state_obj["last_notify_time_stamp"] = None
                return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                               event_alarm.EventAlarmPriority.CLOSE, "", "0020")]
            else:
                # 有正在进行的告警
                return None
        if len(filtered_timeline_items) < 6:
            return None

        surrived_item = [i for i in filtered_timeline_items if
                         (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                          "speed" in i.raw_data and "maxVelocity" in i.raw_data and
                          abs(i.raw_data["speed"]) > i.raw_data["maxVelocity"] and abs(i.raw_data["speed"] > 2.5) and
                          abs(i.raw_data["speed"] < 4))]

        if len(surrived_item) < 6:
            return None

        new_state_obj = {"last_speed_state": "overspeed", "last_speed": filtered_timeline_items[-1].raw_data["speed"],
                         "last_notify_time_stamp": datetime.datetime.now()}
        self.state_obj = new_state_obj

        return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
            datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯超速，当前速度：{}m/s".format(new_state_obj["last_speed"]), "0020")]

    # 运行速度恢复15秒才解除
    def canCloseAlarm(self, filtered_timeline_items, time_diff):
        if time_diff < 15:
            return False

        if time_diff > 120:
            return True

        if len(filtered_timeline_items) < 2:
            return False
        surrived_item = [i for i in filtered_timeline_items if
                         (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                          "speed" in i.raw_data and "maxVelocity" in i.raw_data and
                          abs(i.raw_data["speed"]) > i.raw_data["maxVelocity"])]
        if len(surrived_item) < 3:
            return True
        return False


#
# 反复开关门	判断电梯发生反复开关门故障
class DoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.alarms_to_fire = []
        self.timeline = None
        self.logger = logging.getLogger("doorRepeatlyOpenAndCloseEventDetectorLogger")

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_state_changed_times": [], "last_report_time": None}
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return [i for i in timeline_items if
                    not i.consumed
                    # and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data)
                    and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data
                    and (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 8]

        return filter

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 一次开关门应包括开门跟关门
            if property_name == "door_state":
                if self.state_obj and "last_state_changed_times" in self.state_obj:
                    self.state_obj["last_state_changed_times"].append(
                        datetime.datetime.now())
                    last_state_changed_times: List[datetime.datetime] = self.state_obj["last_state_changed_times"]
                    self.logger.debug("total len of last_state_changed item:{}".format(
                        len(last_state_changed_times)))

                    last_report_time = None if not ("last_report_time" in self.state_obj) else self.state_obj[
                        "last_report_time"]
                    # 已上报过且未结束不再上报
                    if last_report_time:
                        return
                    if len(last_state_changed_times) >= 6:
                        total_time_gap = (last_state_changed_times[-1] - last_state_changed_times[-3]
                                          + last_state_changed_times[-3] - last_state_changed_times[
                                              -5]).total_seconds()
                        config_item = [i for i in self.timeline.configures if i["code"] == "ffkgm"]
                        config_time = 15 if len(config_item) == 0 else int(config_item[0]["value"])
                        if total_time_gap <= config_time:
                            '''
                            last_report_time = None if not ("last_report_time" in self.state_obj) else self.state_obj[
                                "last_report_time"]

                            # 已上报过且未结束不再上报
                            if last_report_time:
                                return

                                report_time_diff = (datetime.datetime.now() - last_report_time).total_seconds()
                                if report_time_diff < 20:
                                    self.logger.debug(
                                        "last report time:{},time_diff:{}".format(last_report_time, report_time_diff))
                                    return
                            '''
                            # self.state_obj["last_state_changed_times"] = []
                            self.alarms_to_fire.append(event_alarm.EventAlarm(
                                self,
                                datetime.datetime.fromisoformat(
                                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                event_alarm.EventAlarmPriority.ERROR,
                                "反复开关门 - 判断电梯发生反复开关门故障,3次开关门的总间隔<=20秒,最近三次 开/关门 从近到远时间分别为: {}  {}  {}".format(
                                    last_state_changed_times[-1].strftime(
                                        "%H:%M:%S"),
                                    last_state_changed_times[-3].strftime(
                                        "%H:%M:%S"),
                                    last_state_changed_times[-5].strftime("%H:%M:%S"))))
                            # self.state_obj["last_state_changed_times"] = []
                        elif len(self.state_obj["last_state_changed_times"]) > 6:
                            self.state_obj["last_state_changed_times"] = self.state_obj["last_state_changed_times"][-6:]
                else:
                    self.state_obj = {"last_state_changed_times": [
                        datetime.datetime.now()]}
                    self.logger.debug(
                        "total len of last_state_changed item:{}".format(len(self.state_obj)))
                # door state values are: OPEN, CLOSE, None
                # data["last_state"]
                # data["new_state"]
                # pass

    # 电梯不运行或15s内电梯状态改变5次
    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # 电梯没有速度信息 不判断反复开关
        if not len(filtered_timeline_items) > 3:
            self.alarms_to_fire.clear()
            return None
        latest_speed_item = filtered_timeline_items[-1]
        detect_speed_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                  latest_speed_item.original_timestamp).total_seconds()
        # 最近一次收到速度信息已经有一段时间，那么无法判断电梯是否停止
        if detect_speed_time_diff > 4:
            self.alarms_to_fire.clear()
            return None

        # 可以关闭，发送结束信息至云端
        if self.state_obj and "last_report_time" in self.state_obj and \
                self.state_obj["last_report_time"] and self.canCloseAlarm(filtered_timeline_items):
            self.alarms_to_fire.clear()
            self.state_obj["last_state_changed_times"] = []
            self.state_obj["last_report_time"] = None
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "004")]

        # 电梯运动不判断反复开关
        if filtered_timeline_items[-3].raw_data["speed"] > 0.3 and \
                filtered_timeline_items[-1].raw_data["speed"] > 0.3:
            self.alarms_to_fire.clear()
            return None

        '''
        if len(self.alarms_to_fire) > 0:
            alarm = self.alarms_to_fire[0]
            target_persons = [i for i in filtered_timeline_items if (alarm.original_utc_timestamp - i.original_timestamp
                                                                     ).total_seconds() > 5 and
                              (alarm.original_utc_timestamp - i.original_timestamp).total_seconds() < 10]
            if len(target_persons) == 0:
                self.alarms_to_fire.clear()
                return None

            self.state_obj["last_report_time"] = datetime.datetime.now()
            self.logger.debug(
                "反复开关门判断中出现人：{},raw:{}".format(target_persons[-1].original_timestamp_str,
                                                          target_persons[-1].raw_data))
        '''
        alarms = self.alarms_to_fire
        if len(self.alarms_to_fire) > 0:
            self.state_obj["last_report_time"] = datetime.datetime.now()
        self.alarms_to_fire = []
        return alarms

    # 电梯运行或30秒门的状态没有改变
    def canCloseAlarm(self, filtered_timeline_items):
        # 电梯运行
        if len(filtered_timeline_items) > 3 and filtered_timeline_items[-3].raw_data["speed"] > 0.3 and \
                filtered_timeline_items[-1].raw_data["speed"] > 0.3:
            return True

        if self.state_obj and "last_state_changed_times" in self.state_obj and \
                len(self.state_obj["last_state_changed_times"]) > 0:
            door_state_change_obj = self.state_obj["last_state_changed_times"]
            if (datetime.datetime.now() - door_state_change_obj[-1]).total_seconds() > 30:
                return True
        return False


#
# 剧烈运动	判断轿厢乘客剧烈运动
class PassagerVigorousExerciseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed and
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data) or
                       (
                               i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "acceleration" in i.raw_data))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj and "last_notify_timestamp" in last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 120:
                return None

        object_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        acceleration_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                                board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        detect_person = None
        for item in reversed(object_filtered_timeline_items):
            if (datetime.datetime.now(datetime.timezone.utc) - item.original_timestamp).total_seconds() < 2:
                detect_person = item
            break

        for item in reversed(acceleration_filtered_timeline_items):
            if item.raw_data["acceleration"] > item.raw_data["maxAcceleration"] and detect_person:
                new_state_obj = {"last_state": "shock", "acceleration": item.raw_data["acceleration"],
                                 "last_notify_timestamp": datetime.datetime.now()}
            break
        if not new_state_obj:
            return None
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "剧烈运动，当前加速度: {}".format(new_state_obj["acceleration"]), "005")]
        return None


#
# 运行中开门	判断电梯发生运行中开门故障
class DoorOpeningAtMovingEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}
        for ed in event_detectors:
            if ed.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                ed.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        self.logger.debug(
            "{} is notified by event from: {} for property: {} with data: {}".format(
                self.__class__.__name__,
                src_detector.__class__.__name__,
                property_name,
                str(data)))
        # 记录上次开门时间
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                property_name == "door_state":
            self.state_obj["door_state"] = {"state": data["new_state"],
                                            "time": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data) or
                    (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                     (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 3)
                    ]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        # 如果告警超过20分钟，可能存在问题暂时关闭
        if last_state_obj and (self.canCloseAlarm(filtered_timeline_items) or (
                datetime.datetime.now() - last_state_obj).total_seconds() > 1200):
            self.state_obj["last_notify_timestamp"] = None
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.CLOSE, "", "0022")]

        if last_state_obj:
            return None

        objects = [i for i in filtered_timeline_items if i.item_type ==
                   board_timeline.TimelineItemType.OBJECT_DETECT]
        if len(objects) == 0:
            return None

        door_state = None if not (self.state_obj and "door_state" in self.state_obj) \
            else self.state_obj["door_state"]
        # 如果没有开门事件，则不认为有在运动中开门的事发生
        if not door_state or door_state["state"] != "OPEN":
            return None

        # 如果开门状态维持超过2分钟，暂时不做运行中开门预警，可能开关门状态不对
        if (datetime.datetime.now(datetime.timezone.utc) - door_state["time"]).total_seconds() > 120:
            return None

        new_state_obj = None
        '''
        if last_state_obj:
            last_notify_time_diff = (
                datetime.datetime.now() - last_state_obj).total_seconds()
            if last_notify_time_diff < 10:
                return None
        '''
        temp_speed = 0.0
        speedItems = [i for i in filtered_timeline_items if
                      i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                      (i.original_timestamp - door_state["time"]).total_seconds() > 0 and
                      abs(i.raw_data["speed"]) > 0.5]
        if len(speedItems) < 5:
            return None

        new_state_obj = datetime.datetime.now()
        '''
        for speed_timeline_item in reversed(speedItems):
            temp_speed = abs(speed_timeline_item.raw_data["speed"])
            door_open_speed_time_diff = (speed_timeline_item.original_timestamp -
                                         door_state["time"]).total_seconds()
            # 获取到的速度值在开门之后
            if temp_speed > 1 and door_open_speed_time_diff > 0:
                new_state_obj = datetime.datetime.now()
            break
        '''
        # 将报警时间存入状态字典
        self.state_obj["last_notify_timestamp"] = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯门在运行中打开,当前速度为:{}m/s".format(temp_speed), "0022")]
        return None

    # 门关上或者电梯停止运行
    def canCloseAlarm(self, filtered_timeline_items):
        if self.state_obj and "door_state" in self.state_obj and self.state_obj["door_state"]["state"] == "CLOSE":
            return True

        speedItems = [i for i in filtered_timeline_items if
                      i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if len(speedItems) < 5:
            return False

        if abs(speedItems[-1].raw_data["speed"]) < 0.3 and abs(speedItems[-2].raw_data["speed"]) < 0.3 and \
                abs(speedItems[-3].raw_data["speed"]) < 0.3 and abs(speedItems[-4].raw_data["speed"]) < 0.3 and abs(
            speedItems[-5].raw_data["speed"]) < 0.3:
            return True
        return False


#
# 急停	判断电梯发生急停故障
class ElevatorSuddenlyStoppedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_notify_timestamp": datetime.datetime.now()}
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data)]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = None
        new_state_obj = None
        if self.state_obj:
            last_state_obj = self.state_obj
        if last_state_obj:
            last_notify_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_notify_time_diff < 300:
                return None

        if len(filtered_timeline_items) < 2:
            return None

        latest_speed_item = filtered_timeline_items[-1]
        previous_speed_item = filtered_timeline_items[-2]

        speed_change_time_diff = (previous_speed_item.original_timestamp -
                                  latest_speed_item.original_timestamp).total_seconds()
        if abs(speed_change_time_diff) > 3 or latest_speed_item.raw_data["speed"] > 4.9:
            return None
        if latest_speed_item and abs(latest_speed_item.raw_data["speed"]) < 0.1 \
                and previous_speed_item and abs(previous_speed_item.raw_data["speed"]) > 1.5:
            new_state_obj = {"current_speed": latest_speed_item.raw_data["speed"],
                             "current_time": latest_speed_item.original_timestamp_str,
                             "previous_speed": previous_speed_item.raw_data["speed"],
                             "previous_time": previous_speed_item.original_timestamp_str,
                             "last_notify_timestamp": datetime.datetime.now()}
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯发生急停,速度从{}的{}m/s在{}变成{}".format(
                                           new_state_obj["previous_time"],
                                           new_state_obj["previous_speed"],
                                           new_state_obj["current_time"],
                                           new_state_obj["current_speed"]), "006")]
        return None


#
# 长时间开门	门开着超过一定时间
class DoorOpenedForLongtimeEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.last_door_open_state_time = None
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_notify_timestamp": None}
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                           (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 3)]
            return result

        return filter

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            if property_name == "door_state":
                # door state values are: OPEN, CLOSE, None
                if data["new_state"] == "OPEN":
                    self.last_door_open_state_time = datetime.datetime.now()
                else:
                    self.last_door_open_state_time = None

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        if len(filtered_timeline_items) == 0:
            return None
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]
            self.logger.debug("board {}， has notified at:{}, door open state:{}".
                              format(self.timeline.board_id, str(self.state_obj["last_notify_timestamp"]),
                                     str(self.last_door_open_state_time)))
        # state_obj 不为空则上报过长时间开门， 而且 last_door_open_state_time为None则认为门关上，告警结束
        if last_state_obj and (
                datetime.datetime.now() - last_state_obj).total_seconds() > 10 and self.last_door_open_state_time is None:
            self.state_obj["last_notify_timestamp"] = None

            result_alarms = [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                    event_alarm.EventAlarmPriority.CLOSE, "长时间开门告警结束", "008")]
            return result_alarms

        # state_obj 不为空怎代表上报过，如果此时的门状态依然为开，那么不重复告警
        if self.state_obj and self.state_obj["last_notify_timestamp"] and self.last_door_open_state_time:
            return None

        config_item = [i for i in self.timeline.configures if i["code"] == "csjkm"]
        config_time = 120 if len(config_item) == 0 else int(config_item[0]["value"])
        if self.last_door_open_state_time \
                and (datetime.datetime.now() - self.last_door_open_state_time).total_seconds() >= config_time:
            alarms = [event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                "长时间处于开门状态 - 门开着超过一定时间的故障, 最近一次门状态转变为'开'的时间为: {}, 且持续时间 >= {}秒".format(
                    self.last_door_open_state_time.strftime(
                        "%d/%m/%Y %H:%M:%S"),
                    (datetime.datetime.now() - self.last_door_open_state_time).total_seconds()))]
            # self.last_door_open_state_time = None
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            return alarms
        return None


#
# 无人高频运行	轿厢内没人, 连续运行15分钟以上
class ElevatorMovingWithoutPeopleInEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        # 最近一次电梯静止时间
        self.state_obj = {"last_notify_timestamp": None, "latest_person_in": datetime.datetime.now()}

        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态. 无人高频运动的情况电梯门应该是一直处于关闭状态
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now()}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data)
                           or (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                               and "speed" in i.raw_data and (datetime.datetime.now(
                                          datetime.timezone.utc) - i.original_timestamp).total_seconds() < 6))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # 更新最近一次有人的时间
        if self.timeline.person_session["person_in"]:
            self.state_obj["latest_person_in"] = datetime.datetime.now()

        last_state_object = self.state_obj["last_notify_timestamp"]

        if last_state_object and self.canCloseAlarm():
            self.state_obj["last_notify_timestamp"] = None
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "0019")]

        # 如果告警还没结束
        if last_state_object:
            return None

        # 电梯静止
        if self.timeline.lift_running_state_session["is_running"] == False:
            return None

        # 电梯运行不足15分钟
        if self.timeline.lift_running_state_session["is_running"] == True and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "session_start_at"]).total_seconds() < 15 * 60:
            return None

        if self.timeline.door_state_session["door_state"] != "closed":
            return None

        # 如果门关上还未超出15分钟
        if self.timeline.door_state_session["door_state"] == "closed" and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.door_state_session[
            "session_start_at"]).total_seconds() < 15 * 60:
            return None

        running_time = (datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "session_start_at"]).total_seconds()
        '''
        if last_state_object:
            last_report_time_diff = (
                datetime.datetime.now() - last_state_object).total_seconds()
            if last_report_time_diff < 120:
                return None
        '''

        # 电梯内有人
        if self.timeline.person_session["person_in"]:
            return None
        # 电梯里的人离开不足15分钟
        if (datetime.datetime.now() - self.state_obj["latest_person_in"]).total_seconds() < 15 * 60:
            return None

        self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
        return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
            datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯无人，高频运行{}秒".format(running_time, "0019"))]

    # 30s内电梯不运行
    def canCloseAlarm(self):
        if self.timeline.lift_running_state_session["is_running"] == False and (
                datetime.datetime.now(datetime.timezone.utc) - self.timeline.lift_running_state_session[
            "session_start_at"]).total_seconds() > 30:
            return True
        return False


#
# 电梯温度过高
class TemperatureTooHighEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        # super().prepare(event_detectors)
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                      and "temperature" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = self.state_obj
        new_state_obj = None
        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_temperature_state": "normal",
                                 "last_notify_timestamp": datetime.datetime.now()}
                break
            elif "temperature" in item.raw_data:
                state = "normal"
                if not isinstance(item.raw_data["temperature"], str) and abs(item.raw_data["temperature"]) > 50:
                    state = str(item.raw_data["temperature"])
                new_state_obj = {"last_temperature_state": state,
                                 "last_notify_timestamp": datetime.datetime.now()}
                break
        if not new_state_obj:
            new_state_obj = {"last_temperature_state": "unknown",
                             "last_notify_timestamp": datetime.datetime.now()}
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 10:
                return None
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj and new_state_obj["last_temperature_state"] != 'unknown' and \
                new_state_obj["last_temperature_state"] != "normal":
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯温度异常，当前温度: {}".format(
                                           new_state_obj["last_temperature_state"]),
                                       "0013")]
        return None


#
# 震动 短时间内，发生正向和反向加速度过大
class ElevatorShockEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                      and "acceleration" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 5:
                return None

        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = None
                break
            elif "acceleration" in item.raw_data and "maxAcceleration" in item.raw_data:
                if abs(item.raw_data["acceleration"]) > abs(item.raw_data["maxAcceleration"]):
                    new_state_obj = {"current_acceleration": item.raw_data["acceleration"],
                                     "configured_max_acceleration": item.raw_data["maxAcceleration"],
                                     "last_notify_timestamp": datetime.datetime.now()}
                break
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯发生震动:当前加速度:{}m/s2，配置的最大加速度为：{}m/s2".format(
                                           new_state_obj["current_acceleration"],
                                           new_state_obj["configured_max_acceleration"]),
                                       "005")]
        return None


# 电梯拥堵
class ElevatorJamsEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger("elevatorJamsEventDetectorLogger")
        self.person_count_list = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.state_obj = {"last_notify_timestamp": None}
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                      and "Person|#" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        # 电梯拥堵配置
        config_item = [i for i in self.timeline.configures if i["code"] == "dtyd"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])

        # 电梯内没人或者人数小于配置数量时清空记录
        if not self.timeline.person_session["person_in"] or self.timeline.person_session[
            "person_count"] <= config_count:
            self.person_count_list = []

        if self.timeline.person_session["person_in"] and self.timeline.person_session["person_count"] > config_count:
            self.person_count_list.append(self.timeline.person_session["person_count"])

        if last_state_obj:
            last_report_time_diff = (datetime.datetime.now() - last_state_obj).total_seconds()
            if (not self.timeline.person_session["person_in"] or self.timeline.person_session[
                "person_count"] < config_count) and last_report_time_diff > 10:
                self.state_obj["last_notify_timestamp"] = None
                return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                               event_alarm.EventAlarmPriority.CLOSE, "电梯拥堵结束", "0011")]
            else:
                return None

        # 电梯内没人或者人数小于电梯拥堵配置的人数
        # if not self.timeline.person_session["person_in"] or self.timeline.person_session["person_count"] < config_count:
        if len(self.person_count_list) < 3:
            return None

        # 人数大于配置人数
        if self.timeline.person_session["person_count"] > config_count:
            if not self.state_obj:
                self.state_obj = {}
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯拥堵:当前人数:{}".format(self.timeline.person_session["person_count"]),
                                       "0011")]
        return None


#
# 电梯里程
class ElevatorMileageEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                                   and "speed" in i.raw_data
                           ) or (
                                   i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = None
        new_state_obj = None
        alarms = []
        if self.state_obj and "elevator_state" in self.state_obj:
            last_state_obj = self.state_obj["elevator_state"]
        if len(filtered_timeline_items) > 0:
            speed_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                             board_timeline.TimelineItemType.SENSOR_READ_SPEED]
            storey_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                              board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
            if len(speed_filtered_timeline_items) > 0 and len(storey_filtered_timeline_items) > 0 and \
                    speed_filtered_timeline_items[-1].board_msg_id == storey_filtered_timeline_items[-1].board_msg_id:
                current_speed_item = speed_filtered_timeline_items[-1]
                current_storey_item = storey_filtered_timeline_items[-1]
                if last_state_obj:
                    state_len = len(last_state_obj)
                    if len(last_state_obj) == 1:
                        if current_speed_item.raw_data["speed"] != 0:
                            last_state_obj.append({"speed": current_speed_item.raw_data["speed"],
                                                   "storey": current_storey_item.raw_data["storey"],
                                                   "timestamp": datetime.datetime.now(datetime.timezone.utc)})
                        else:
                            last_state_obj[0]["timestamp"] = datetime.datetime.now(
                                datetime.timezone.utc)
                    elif len(last_state_obj) == 2:
                        if current_speed_item.raw_data["speed"] == 0:
                            last_state_obj.append({"speed": current_speed_item.raw_data["speed"],
                                                   "storey": current_storey_item.raw_data["storey"],
                                                   "timestamp": datetime.datetime.now(datetime.timezone.utc)})
                        else:
                            last_state_obj[1]["timestamp"] = datetime.datetime.now(
                                datetime.timezone.utc)
                else:
                    last_state_obj = [{"speed": current_speed_item.raw_data["speed"],
                                       "storey": current_storey_item.raw_data["storey"],
                                       "timestamp": datetime.datetime.now(datetime.timezone.utc)}]
        if last_state_obj and len(last_state_obj) == 3:
            floor_count = abs(
                last_state_obj[0]["storey"] - last_state_obj[2]["storey"])
            passenger_count = 0
            persons = [i for i in filtered_timeline_items if i.item_type ==
                       board_timeline.TimelineItemType.OBJECT_DETECT and
                       (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 3]
            if len(persons) > 0:
                latest_person_item = persons[-1]
                targets = [i for i in persons if i.original_timestamp_str ==
                           latest_person_item.original_timestamp_str]
                passenger_count = len(targets)
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.INFO,
                "floor_count:{},passenger_count:{},start_date:{},end_date:{}".format(floor_count, passenger_count,
                                                                                     last_state_obj[0]["timestamp"],
                                                                                     last_state_obj[2]["timestamp"]),
                "TRIP", {"floor_count": str(floor_count),
                         "passenger_count": str(passenger_count),
                         "start_date": str(last_state_obj[0]["timestamp"]),
                         "end_date": str(last_state_obj[2]["timestamp"])}))
            last_state_obj.pop(0)
            last_state_obj.pop(0)
        self.state_obj["elevator_state"] = last_state_obj
        return alarms


#
# 电梯运行状态 上行、下行 停止 所在楼层
class ElevatorRunningStateEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []
        self.door_state = ""

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.door_state = data["new_state"]

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # and (datetime.datetime.now() - i.local_timestamp).total_seconds() <= 10
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                                   and "speed" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                                   and "Person|#" in i.raw_data
                           ) or i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT)]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        # 下行时速度>0 上行时速度<0
        if len(filtered_timeline_items) > 0:
            person_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                     board_timeline.TimelineItemType.OBJECT_DETECT]
            speed_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                    board_timeline.TimelineItemType.SENSOR_READ_SPEED]
            storey_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                     board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
            sensor_detect_person_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT]
            sensor_detect_person = False
            sensor_detect_person_in_lift = False
            if len(sensor_detect_person_items) > 0:
                sensor_detect_person = sensor_detect_person_items[-1].raw_data["detectPerson"]
                sensor_detect_person_in_lift = str(False) if "detectPersonInLift" not in sensor_detect_person_items[
                    -1].raw_data else \
                    sensor_detect_person_items[-1].raw_data["detectPersonInLift"]
            hasPerson = "Y" if len(person_timeline_items) > 0 else "N"
            person_count = 0
            if len(person_timeline_items) > 0:
                latest_person_object = filtered_timeline_items[-1]
                target_persons = [i for i in filtered_timeline_items if
                                  i.board_msg_id == latest_person_object.board_msg_id
                                  and i.original_timestamp_str == latest_person_object.original_timestamp_str]
                person_count = len(target_persons)

            code = ""
            speed = 0.00 if not len(speed_timeline_items) > 0 else speed_timeline_items[-1].raw_data["speed"]
            acceleration = 0.00 if not len(speed_timeline_items) > 0 else speed_timeline_items[-1].raw_data[
                "acceleration"]

            # LIFTUP LIFTDOWN
            if len(speed_timeline_items) > 4:
                last_speed_object = speed_timeline_items[-1]
                previous_speed_oject = speed_timeline_items[-2]
                speed = last_speed_object.raw_data["speed"]
                if abs(last_speed_object.raw_data["speed"]) < 0.2 and \
                        abs(previous_speed_oject.raw_data["speed"]) < 0.2:
                    code = "LIFTSTOP"
                # 如果电梯在运动，比较楼层的变化判断向上、向下
                elif not (abs(last_speed_object.raw_data["speed"]) < 0.2):
                    storey_latest = storey_timeline_items[-1].raw_data["storey"]
                    storey_last_fifth = storey_timeline_items[-5].raw_data["storey"]
                    code = "LIFTDOWN" if storey_latest < storey_last_fifth else "LIFTUP"

            storey = 0 if not len(storey_timeline_items) > 0 else storey_timeline_items[-1].raw_data["storey"]
            pressure = 0 if not len(storey_timeline_items) > 0 else storey_timeline_items[-1].raw_data["pressure"]
            if last_state_object and "code" in last_state_object:
                if last_state_object["code"] == code and last_state_object["floor"] == storey:
                    # last_state_object["hasPerson"] == hasPerson:
                    return None
            if code != "":
                self.state_obj = {"code": code, "floor": storey, "hasPerson": hasPerson,
                                  "time_stamp": datetime.datetime.now()}
                # if last_state_object and last_state_object["code"] == code and last_state_object["floor"] == \
                #        storey and last_state_object["hasPerson"] == hasPerson:
                if last_state_object and "time_stamp" in last_state_object:
                    report_diff = (datetime.datetime.now(
                    ) - last_state_object["time_stamp"]).total_seconds()
                    if report_diff < 2:
                        return None
                time = datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat())
                '''
                data = {"human_inside": hasPerson, "floor_num": str(storey)} if code == "LIFTSTOP" \
                    else {"human_inside": hasPerson, "floor_num": str(storey), "speed": str(speed)}
                '''
                guang_dian = 1 if storey == 1 else 0
                # model7 识别人数 model8:轿厢顶人体感应 model10:光电感值 model4:液晶屏楼层 model2门状态
                data = {"model1": storey, "model2": self.door_state, "model3": code, "model4": storey,
                        "model5": pressure,
                        "model6": speed, "model7": person_count, "model8": sensor_detect_person,
                        "model9": acceleration,
                        "model10": guang_dian,
                        "model11": sensor_detect_person_in_lift,
                        "createdDate": str(time),
                        "liftId": self.timeline.liftId}

                alarms.append(event_alarm.EventAlarm(
                    self, time,
                    event_alarm.EventAlarmPriority.INFO,
                    "human_inside:{},floor_num:{},speed:{}".format(
                        hasPerson, storey, speed),
                    "general_data", data))
        return alarms


#
# 陀螺仪故障 1，获取不到加速度 2，连续5次读到的加速度值相同（正常情况下，即使不动每次读取的值也不一样）
class GyroscopeFaultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                      and "raw_acceleration" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 4:
            return None
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            if last_report_time_diff < 600:
                return None
        config_item = [i for i in self.timeline.configures if i["code"] == "tlygz"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        target_timeline_items = filtered_timeline_items[-1 * config_count:]
        # 如果读取到的加速度值一直是零/相同
        latest_raw_acceleration = target_timeline_items[-1].raw_data["raw_acceleration"]
        result_timeline_items = [i for i in target_timeline_items if i.raw_data["raw_acceleration"] ==
                                 latest_raw_acceleration]
        if len(result_timeline_items) == config_count:
            msg = "获取不到加速度" if latest_raw_acceleration == 0.000 else "连续五次获取到的加速度值相同"
            self.state_obj = {"code": "TLYJG", "message": msg,
                              "time_stamp": datetime.datetime.now()}
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                msg,
                "TLYJG"))
        return alarms


#
# 光电感故障 1，获取不到温度值/气压值 2，连续5次读到的气压值相同（正常情况下，即使不动每次读取的值也不一样）
# 3，收到光电信号时，气压值连续3次不在1楼标定的气压范围内
class PressureFaultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 4:
            return None
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            if last_report_time_diff < 600:
                return None
        config_item = [i for i in self.timeline.configures if i["code"] == "qyjgz"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        target_timeline_items = filtered_timeline_items[-1 * config_count:]
        # 如果读取到的气压值一直是零/相同
        latest_raw_pressure = target_timeline_items[-1].raw_data["pressure"]
        result_timeline_items = [i for i in target_timeline_items if i.raw_data["pressure"] ==
                                 latest_raw_pressure]
        if len(result_timeline_items) == config_count:
            msg = "获取不到气压值" if latest_raw_pressure == 0.000 else "连续五次获取到的气压值相同"
            new_state_object = {"code": "TLYJG", "message": msg,
                                "time_stamp": datetime.datetime.now()}
        result_timeline_items = [
            i for i in target_timeline_items if i.raw_data["temperature"] == 0.00]
        if len(result_timeline_items) == config_count:
            msg = "气压计获取不到温度"
            if new_state_object:
                new_state_object["message"] = new_state_object["message"] + ";" + msg
            else:
                new_state_object = {
                    "code": "QYJGZ", "message": msg, "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "QYJGZ"))
        return alarms


# 边缘计算超过1天时长获取的光电感信号一直为1
# 冲顶报警，该判断放在边缘，只上传判断结果1，气压判断楼层为最高层 2，光电感未触发
# 光电感应故障，直接在边缘端判断
# 蹲底报警：电梯运行至最底层，但最底层光电感未触发，判断逻辑在边缘
class ElectricSwitchFaultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []
        self.state_obj_CD: dict = None
        # 蹲底
        self.state_obj_DD: dict = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_ELECTRIC_SWITCH]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        # last_state_object = self.state_obj
        if not len(filtered_timeline_items) > 0:
            return None

        for item in filtered_timeline_items:
            item.consumed = True

        target_timeline_item = filtered_timeline_items[-1]
        need_report_state_obj = target_timeline_item.raw_data["switchFault"]
        need_report_state_obj_cd = target_timeline_item.raw_data["highestLevelSwitchError"]
        need_report_state_obj_dd = target_timeline_item.raw_data["lowestLevelSwitchError"]
        if self.state_obj and target_timeline_item.raw_data["switchFault"]:
            last_report_time_diff = (
                    datetime.datetime.now() - self.state_obj["time_stamp"]).total_seconds()
            # 光电故障6小时报一次，不用太频繁
            if last_report_time_diff < 21600:
                need_report_state_obj = False

        if self.state_obj_CD and target_timeline_item.raw_data["highestLevelSwitchError"]:
            last_report_time_diff = (
                    datetime.datetime.now() - self.state_obj_CD["time_stamp"]).total_seconds()
            # 光电故障6小时报一次，不用太频繁
            if last_report_time_diff < 21600:
                need_report_state_obj_cd = False

        if self.state_obj_DD and target_timeline_item.raw_data["lowestLevelSwitchError"]:
            last_report_time_diff = (
                    datetime.datetime.now() - self.state_obj_DD["time_stamp"]).total_seconds()
            # 光电故障6小时报一次，不用太频繁
            if last_report_time_diff < 21600:
                need_report_state_obj_dd = False

        if not need_report_state_obj_cd and not need_report_state_obj and not need_report_state_obj_dd:
            return None

        # 光电感故障
        if target_timeline_item.raw_data["switchFault"]:
            self.state_obj = {
                "code": "GDGXSYC", "message": "光电感已超过1天未触发", "time_stamp": datetime.datetime.now()}
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                self.state_obj["message"],
                "GDGXSYC"))
        # 冲顶
        elif target_timeline_item.raw_data["highestLevelSwitchError"]:
            self.state_obj_CD = {
                "code": "CD", "message": "电梯运行至最高层，最高层光电感未触发", "time_stamp": datetime.datetime.now()}
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                self.state_obj_CD["message"],
                "CD"))
        # 蹲底
        elif target_timeline_item.raw_data["lowestLevelSwitchError"]:
            self.state_obj_DD = {
                "code": "DD", "message": "电梯运行至底层，底层光电感未触发", "time_stamp": datetime.datetime.now()}
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                self.state_obj_DD["message"],
                "DD"))
        return alarms


# 边缘板子软件包更新结果
class UpdateResultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.UPDATE_RESULT]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None
        target_timeline_item = filtered_timeline_items[-1]
        target_timeline_item.consumed = True
        if target_timeline_item.raw_data['update']:
            return None
        new_state_object = {"code": "UPGRADEERROR",
                            "message": target_timeline_item.raw_data['description'],
                            "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                new_state_object["message"],
                new_state_object["code"]))
        return alarms


# 边缘板子离线
class DeviceOfflineEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        # 启动时假设边缘板子在线
        self.state_obj = {
            "detect_device_time_stamp": datetime.datetime.now(datetime.timezone.utc)}

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None
        if last_state_object and "notify_time" in last_state_object:
            notify_time_diff = (datetime.datetime.now(
            ) - last_state_object["notify_time"]).total_seconds()
            if notify_time_diff < 600:
                return None

        target_timeline_item = [i for i in filtered_timeline_items if
                                not i.consumed
                                and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED or
                                     i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT)]
        if target_timeline_item and len(target_timeline_item) > 0:
            latest_speed_item = target_timeline_item[-1]
            self.state_obj["detect_device_time_stamp"] = latest_speed_item.original_timestamp

        time_diff = (datetime.datetime.now(datetime.timezone.utc) - self.state_obj[
            "detect_device_time_stamp"]).total_seconds()
        config_item = [i for i in self.timeline.configures if i["code"] == "sblx"]
        config_count = 6 if len(config_item) == 0 else int(config_item[0]["value"])
        if time_diff < config_count * 60:
            return None
        new_state_object = {"code": "0023",
                            "message": "边缘设备离线，最近一次获取到边缘设备信息的时间为{}".format(
                                self.state_obj["detect_device_time_stamp"].strftime("%d/%m/%Y %H:%M:%S")),
                            "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj["notify_time"] = datetime.datetime.now()
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                new_state_object["message"],
                new_state_object["code"]))
        return alarms


# 光电感应故障,检测到电梯顶部有人
class DetectPersonOnTopEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        config_item = [i for i in self.timeline.configures if i["code"] == "jdyr"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        if not len(filtered_timeline_items) > config_count:
            return None
        # target_timeline_item = filtered_timeline_items[-1]
        # target_timeline_item.consumed = True
        detect_person = "Y"
        description = "{}测到轿顶有人".format(
            datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        for item in reversed(filtered_timeline_items):
            item.consumed = True
            if "detectPerson" in item.raw_data and not item.raw_data["detectPerson"]:
                detect_person = "N"
                description = "{}未检测到轿顶有人".format(
                    datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

        # if "detectPerson" in target_timeline_item.raw_data:
        #    detect_person = "Y" if target_timeline_item.raw_data["detectPerson"] else "N"
        #    description = "未检测到轿顶有人" if detect_person == "N" else "检测到轿顶有人进入"
        new_state_object = {"code": "HUMANONTOP", "detectPerson": detect_person, "message": description,
                            "time_stamp": datetime.datetime.now()}
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            # 检测顶部有人5分钟报一次
            if last_report_time_diff < 300 and "detectPerson" in last_state_object and \
                    last_state_object["detectPerson"] == detect_person:
                return None
        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "HUMANONTOP", {"human_on_top": detect_person}))
        return alarms


# ISAP 检测到摄像头被遮挡事件
class DetectCameraBlockedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.CAMERA_BLOCKED]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None

        target_timeline_item = filtered_timeline_items[-1]
        target_timeline_item.consumed = True
        if "cameraBlocked" in target_timeline_item.raw_data:
            if target_timeline_item.raw_data["cameraBlocked"]:
                description = "摄像头被遮挡，检测到的时间为：{}".format(
                    target_timeline_item.local_utc_timestamp.strftime("%d/%m/%Y %H:%M:%S"))
                new_state_object = {"code": "009", "cameraBlocked": True, "message": description,
                                    "time_stamp": datetime.datetime.now()}
        if last_state_object:
            if new_state_object == None:
                self.state_obj = None
                # self.sendMessageToKafka("|TwoWheeler|confirmedExit")
                alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                     event_alarm.EventAlarmPriority.CLOSE,
                                                     "close camera blocked alarm", "009"))
                return alarms
            else:
                return None

        if new_state_object:
            self.state_obj = new_state_object

            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "009"))
            '''
            self.sendMessageToKafka("Vehicle|#|TwoWheeler" + "|TwoWheeler|confirmed")
            alarms.append(
                event_alarm.EventAlarm(self,
                                       datetime.datetime.fromisoformat(
                                           datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "detected electric-bicycle due to camera block event", "007", {}))
            '''
            self.logger.debug("cameraBlocked boardId:{}".format(self.timeline.board_id))
        return alarms

    def sendMessageToKafka(self, message):
        try:
            config_item = [i for i in self.timeline.configures if i["code"] == "kqzt"]
            config_kqzt = 1 if len(config_item) == 0 else int(config_item[0]["value"])
            if self.timeline.board_id in self.timeline.target_borads or config_kqzt == 0:
                return
            # self.logger.info("------------------------board:{} is not in the target list".format(self.timeline.board_id))
            # producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
            #                         value_serializer=lambda x: dumps(x).encode('utf-8'))
            obj_info_list = []
            obj_info_list.append(message)
            self.timeline.producer.send(self.timeline.board_id + "_dh", {
                'version': '4.1',
                'id': 1913,
                '@timestamp': '{}'.format(datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                'sensorId': '{}'.format(self.timeline.board_id + "_dh"), 'objects': obj_info_list})
            # self.timeline.producer.flush(5)
            # producer.close()
        except:
            self.logger.exception(
                "send electric-bicycle confirmed message to kafka(...) rasised an exception:")


# 门标异常
# 报警条件：连续20分钟检测不到门标
# 关闭条件：连续5次检测到门标
class DetectDoorWarningSignLostEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner even
        """
        self.timeline = timeline
        # 启动时假设门标
        self.state_obj = {"detect_door_warning_sign_time_stamp": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                      (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 10]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        result = [i for i in filtered_timeline_items if
                  not i.consumed
                  and "DoorWarningSign" in i.raw_data]
        if len(result) > 0:
            self.state_obj["detect_door_warning_sign_time_stamp"] = filtered_timeline_items[-1].original_timestamp

        last_notify_time = None
        if self.state_obj and "notify_time" in self.state_obj and self.state_obj["notify_time"]:
            last_notify_time = self.state_obj["notify_time"]

        # 关闭告警
        if last_notify_time and len(result) > 4:
            self.state_obj["notify_time"] = None
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "DOORWARNINGSIGN")]

        if last_notify_time:
            return None

        alarms = []
        # config_item = [i for i in self.timeline.configures if i["code"] == "ffkgm"]
        config_item = []
        config_time = 1200 if len(config_item) == 0 else int(config_item[0]["value"])
        if ((datetime.datetime.now(datetime.timezone.utc) - self.state_obj[
            "detect_door_warning_sign_time_stamp"]).total_seconds() > config_time):
            self.state_obj["notify_time"] = datetime.datetime.now()
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                "门标异常，连续20分钟未检测到门标：{}".format(
                    self.state_obj["detect_door_warning_sign_time_stamp"].strftime("%d/%m/%Y %H:%M:%S")),
                "DOORWARNINGSIGN"))
        return alarms


# 一键呼救
# 一键呼救按钮被按时，边缘信息中将包含SOS buttonPressed= true
# 此告警无需关闭
class SOSEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []
        self.state_obj_CD: dict = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SOS_BUTTON]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        # last_state_object = self.state_obj
        if not len(filtered_timeline_items) > 0:
            return None

        for item in filtered_timeline_items:
            item.consumed = True

        target_timeline_item = filtered_timeline_items[-1]

        need_report_state_obj = target_timeline_item.raw_data["buttonPressed"]
        if self.state_obj and target_timeline_item.raw_data["buttonPressed"]:
            last_report_time_diff = (
                    datetime.datetime.now() - self.state_obj["time_stamp"]).total_seconds()
            # 一键呼救10分钟内不反复上传
            if last_report_time_diff < 600:
                need_report_state_obj = False

        if not need_report_state_obj:
            return None

        # 一键呼叫按钮触发
        if target_timeline_item.raw_data["buttonPressed"]:
            self.state_obj = {
                "code": "SOS", "message": "一键呼救按钮触发", "time_stamp": datetime.datetime.now()}
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                self.state_obj["message"],
                "SOS"))
        return None


#
# 电梯的 通过本系统气压计算出来的楼层 与 电梯自带的楼层显示屏显示楼层 不一致
class CaculatedFloorNumberDifferentFromEleFloorScreenNumberEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger: Logger = logging.getLogger(
            "CaculatedFloorNumberDifferentFromEleFloorScreenNumberEventDetectorLogger")
        self.statistics_logger = logging.getLogger("statisticsLogger")

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.inferencer = Inferencer(
            self.statistics_logger, self.timeline.board_id)
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                           and "storey" in i.raw_data)]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj and "last_notify_timestamp" in last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 120:
                return None

        story_filtered_timeline_items = [i for i in filtered_timeline_items if
                                         i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]

        if len(story_filtered_timeline_items) > 0:
            self.current_storey = story_filtered_timeline_items[-1].raw_data["storey"]
        return None

    def on_mqtt_message_from_board_outbox(self, mqtt_message: paho_mqtt_client.MQTTMessage):
        """
        the callback function when a mqtt message is received from board,
        mostly used for receive the response from board for confirm the previous request
        has been received and processed in board.
        """
        str_msg = mqtt_message.payload.decode("utf-8")
        """
        sample of event for type of "ele_idle_floor_screen_snapshot"---->
        {
  "event": {
    "id": "",
    "sender": "edge_board_app_elenet",
    "timestamp": "2025-02-14T02:28:19.221458+00:00",
    "overall_status_code": 200,
    "type": "ele_idle_floor_screen_snapshot",
    "data": {
      "image_base64": "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAIBAQEBAQIBAQECAgICAgQDAgICAgUEBAMEBgUGBgYFBgYGBwkIBgcJBwYGCAsICQoKCgoKBggLDAsKDAkKCgr/2wBDAQICAgICAgUDAwUKBwYHCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgoKCgr/wAARCABUAEMDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD4XmLK2KdbPsdZJCQvPOOmAT/Q0srpJJ8vrWd8QLm40v4VeJtVs5Nklvok5DjqpKlQfzYD8aAG6Z8b/hFqmEHjlEPbfZSx/wDoQ5rqdJ1nwZq6iXS/GdlNjoPtSqT+ZFfGMTM8e0uuCBw0Yb+dOSGBQ00P7sKeqNgj8qAPvDS726tUK2koKN1IwwNWnluGgWeRcMemFC5/AV8M6Z4n8UaKrf2T4v1KAP8AeEd/J/InFbuiftA/Grw3KRafECW5jDApBeWcLr+YQN+tAH3Bo2paxc6NLay6eLeZoz5MssbMme2doJNM0298TRlL20gspV/hlC8frXy34f8A24PirpwY63penXIJHlmBpItg7/xNmtyx/bbsbyf7R4i8AmSQ9XjmQ/8AoXNAHvN7GtxdPNe6TaGVj85Ejjn6A4orzrT/AIyeGvFFmmvWVq0cVwCVTJ4wcHrz1FFAEZERO5SM9+a5b9ojVp9H/Z+1/wCy3XltfyWlkVEe7zN1wkpXpxxCxzx0xnnB20uzEdxHFefftW65LD8OdI0SK5AXUtejleHHLrEkmT+BkH/fVAHh8EzR24DHnHpUnmMihShBPIyOPrTMYfy2II6YpVTGVD5+QZAH1oAcLgk7T25p0Qkf7v8AKkhtt2GAwD2NXLe3KDcCOEJ/GgBnlsIizp071DDIJGIBA/Gtie0VrRh3K8c1nxaVLKI7Syi3XE0qxxruxuJOAOeOpoA94+FHhTTovhzpH9r3phuZLQSTRKwYIzktjIODjNFbdvb21nbx2cUXyQxrGgz0VQAB+QooAWOUyKVJ615d+1Jexy6j4U03IJi02efGOm9oxn/yGfyr06JD9mZx1214f+0JqF1e/E62tGlzHY6NbQouPusdzt/6EKAOS8oyT4UDkd/WrVtDI0Y8zbgDqcZp8MEXlpKw+bb1q7p6iaQBgTng80AQQWmGxnOeemMe1aFrYIdr7MMOhrZg0K18iOXygN/3cty3+NLd6VPp6ea9q4XPGVoAx7pHG0cD1Hb/AOvVnwjppvfGWlRAcfa1dvovP+FQ3cnmzhSMAGtDwEzxeJzqKtxaW8hUe5xj+VAHqUeqCRA5bqPWisCG5lESjeBx0OaKAOuSSR4DCp+YjAwK8H+Jskmo/EHW72Q58nUTApx2RVUD8q9/06MfbEVvXj8q+b77UZdTlv8AVJn3NdajNMz9N2XOD+QFAEVmQ5KMuNtb/h6yS5mWEAZPoayrG1WSBZVAyRk103w/0ye91EGBQWQZGeme1AH3J/wSq/Y3+Gv7Q+p3d/8AEPXbG2i8PbLq5t7mT99c25Yh1ijOfMxkZCgtg8V1H/BX39nv9lz4JrokfwD8M3ttFNp8X21ri38oTTOqneBIN4B54PHIx3r03/gjn8A/in4r0qX42+E/A9zc6d4f1G3RbvTp4hbyXqpKPJK3LDeMFzuwyqcZIyM8T/wWwvPGd140tpfGPh+/061uo2OmWuooFMYRgNq4dgVHABXaD/dFAH5papapHfFU6Bjg1peC7VUM1zx87lefQVFrFvm7ARep4FXfD1vLb2KFtuTI5YZ5HIoA14JSIVDHnHPFFQo42jDjFFAHZ69qcum6Zd6xAyKLS2lYsnOG2HHX3xXz9HEqad9nON2417X4mvDqWh3GhTXEkMNwmyWWMK25fTB5H4YNcBe/DeD7QX03xHHKh5G+FlOaAObglMcKQjrjFb3hW+/syZb1nwFPI7E01vA+oQuGLo3uDVW50XVraYoiM0ffC5oA+zP2If8AgrR+0L+xXoGqeEPhN4te00fVmWWfT5LW3uYknXdtlRZ438s/M2QmA2RuB2rjI/4KLf8ABRv4t/t8eNdO8VfE3xAGj0ay+yabpNpAsdnafd3tCoG4b9oJznJUYr5Oha4tgsTZDEcbhgfn0olu5kO1txDHNAFy5u5pmeY5Yr0VcZP0rY06Y2cUUZi3iOMHhvvE9axbIDzFYnA781prOUnLxbGBUDn2oA0Iy4QD5enrRVZZhjk4P0ooAa1zOA6GUkEjg1BcOxTG4j6UUUAXNDgWbT7y8kZi9uYxHzx8xbOfyrbsJPIskMcaZIOSUBzz70UUAQypbyRMJLOJgRypTrXN3tpbo5CwqBnoBRRQBBaxqHBHr61diGYiT60UUAMaR1YgN0Y9veiiigD/2Q=="
    },
    "description": ""
  }
}
        """
        # try handle the ele_idle_floor_screen_snapshot event
        try:
            event = json.loads(str_msg)
            if "event" in event and "type" in event["event"] and event["event"][
                "type"] == "ele_idle_floor_screen_snapshot":
                if "data" in event["event"] and "image_base64" in event["event"]["data"]:
                    # do something with the image_base64
                    image_base64_str = event["event"]["data"]["image_base64"]
                    try:
                        if self.state_obj and "last_notify_timestamp" in self.state_obj:
                            last_report_time_diff = (
                                    datetime.datetime.now() - self.state_obj["last_notify_timestamp"]).total_seconds()
                            if last_report_time_diff < 60 * 60 * 24:
                                return

                        llm_result = self.inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
                            ["data:image," + image_base64_str], model_name="qwen-vl-max-0809",
                            user_prompt="请回答",
                            system_prompt="你是一个数字识别专家,此图片是电梯中的楼层显示屏,里面显示的楼层数是多少?必须以json格式返回结果,例如: {'floor_number': 1999}")
                        inferenced_floor_number = llm_result["floor_number"]
                        self.logger.debug("Inferenced floor number is: {}".format(inferenced_floor_number))
                        if self.current_storey != inferenced_floor_number:
                            self.logger.info(
                                "Current storey is: {}, inferenced floor number is: {}, will raise alarm".format(
                                    self.current_storey, inferenced_floor_number))
                            new_state_obj = {"last_state": "different", "current_storey": self.current_storey,
                                             "inferenced_floor_number": inferenced_floor_number,
                                             "last_notify_timestamp": datetime.datetime.now()}
                            self.state_obj = new_state_obj
                            self.timeline.notify_event_alarm(
                                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                       event_alarm.EventAlarmPriority.WARNING,
                                                       "电梯的 通过本系统气压计算出来的楼层 与 电梯自带的楼层显示屏显示楼层 不一致, 通过本系统计算的楼层为: {}, 电梯自带的楼层显示屏显示的楼层为: {}".format(
                                                           inferenced_floor_number, self.current_storey),
                                                       "CaculatedFloorNumberDifferentFromEleFloorScreenNumber"))
                    except:
                        self.logger.exception("handle ele_idle_floor_screen_snapshot event raised an exception")
        except:
            self.logger.exception("handle ele_idle_floor_screen_snapshot event raised an exception:")
