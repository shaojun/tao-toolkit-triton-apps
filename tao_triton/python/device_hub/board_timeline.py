from __future__ import annotations
import datetime
from json import dumps
import json
import time
from enum import Enum
from typing import Any, Callable, List, Literal

from kafka import KafkaProducer
import paho.mqtt.client as paho_mqtt_client
from tao_triton.python.device_hub.event_session.manager import ElectricBicycleInElevatorSession


class TimelineItemType(int, Enum):
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
                 raw_data: str,
                 is_frame_grayscale=False,
                 version=""):
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
        # 用于目标识别，暂放此处，当前识别物体是否在灰度情况下识别到
        self.is_frame_grayscale = is_frame_grayscale
        # version 主要用来辨别是否来自旷明主板4.1qua
        self.version = version


class BoardTimeline:
    # by seconds
    Timeline_Items_Max_Survive_Time = 120

    def __init__(self, logging, board_id: str, items: list[TimelineItem], event_detectors,
                 event_alarm_notifiers: list, producer, mqtt_client: paho_mqtt_client.Client, target_borads: str, lift_id: str):
        self.last_state_update_local_timestamp = None
        self.logger = logging.getLogger("boardTimeline")
        self.board_id = board_id
        self.items = items
        self.event_detectors = event_detectors
        self.event_alarm_notifiers = event_alarm_notifiers
        self.target_borads = target_borads
        self.liftId = lift_id
        self.configures = []
        self.producer = producer
        self.mqtt_client = mqtt_client
        self.ebik_session = ElectricBicycleInElevatorSession(logging, self)

        self.person_session = {"person_in": False, "session_start_at": None, "latest_person_item_time": None,
                               "person_count": 0}

        self.door_state_session = {"door_state": "unkown", "session_start_at": None,
                                   "latest_current_state_item_time": None,
                                   "door_closed_no_doorsign_count": 0,
                                   "is_frame_grayscale": True, "latest_grayscal_time": None}

        self.lift_running_state_session = {"is_running": False,
                                           "session_start_at": datetime.datetime.now(datetime.timezone.utc),
                                           "latest_current_state_item_time": datetime.datetime.now(
                                               datetime.timezone.utc),
                                           "item_data": 0}
        self.mqtt_board_inbox_topic = f"board/{self.board_id}/elenet/inbox"

        for d in event_detectors:
            d.prepare(self, event_detectors)

    def on_mqtt_message_from_board_outbox(self, client, userdata, mqtt_msg) -> Callable[[paho_mqtt_client.Client, Any, paho_mqtt_client.MQTTMessage], None]:
        self.logger.debug("board: {}, received mqtt message from board outbox: {}".format(
            self.board_id, mqtt_msg.payload.decode()))
        for d in self.event_detectors:
            # mqtt_message_filter_function = getattr(
            #     d, "mqtt_message_filter", None)
            detector_on_mqtt_message_from_board_outbox_function = getattr(
                d, "on_mqtt_message_from_board_outbox", None)
            try:
                if callable(detector_on_mqtt_message_from_board_outbox_function):
                    detector_on_mqtt_message_from_board_outbox_function(
                        mqtt_msg)
            except Exception as e:
                self.logger.exception(
                    "board: {}, call detector_on_mqtt_message_from_board_outbox_function: {} from timeline raised an exception: {}".format(d.__class__.__name__, self.board_id, e))

    def send_mqtt_message_to_board_inbox(self, msg_id: str,
                                         action_type: Literal['enable_block_door', 'disable_block_door'],
                                         action_data: dict = None,
                                         description: str = None) -> bool:
        try:
            config_item = [
                i for i in self.configures if i["code"] == "kqzt"]
            config_kqzt = 1 if len(config_item) == 0 else int(
                config_item[0]["value"])
            config_close_zt = [
                i for i in self.configures if i["code"] == "gbzt"]
            gbzt = False if len(config_close_zt) == 0 else (
                self.board_id in config_close_zt[0]["value"])
            if (config_kqzt == 0 or gbzt) and "enable_block_door" in action_type:
                self.logger.info("board:{}, configured to disable the feature of block door, so won't send request with action_type: 'enable_block_door' to board".format(
                    self.board_id))
                return
            # self.logger.info("------------------------board:{} is not in the target list".format(self.timeline.board_id))
            # producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
            #                         value_serializer=lambda x: dumps(x).encode('utf-8'))
            self.logger.debug("board: {}, sending mqtt request with msg_id: '{}', action_type: '{}' to board".format(
                self.board_id, msg_id, action_type))
            request = {"id": msg_id,
                       "sender": "devicehub",
                       "timestamp": f"{datetime.datetime.now().isoformat()}",
                       "description": description,
                       "type": action_type,
                       "body": action_data}
            publish_result = self.mqtt_client.publish(
                self.mqtt_board_inbox_topic, json.dumps(request), qos=1)
            status = publish_result[0]
            if status == 0:
                # print(f"Send `{msg}` to topic `{topic}`")
                return True
            else:
                self.logger.error(
                    f"board:{self.board_id}, failed to send mqtt request: '{request}' to topic: {self.mqtt_board_inbox_topic}")
                return False
        except Exception as e:
            self.logger.exception(
                f"board:{self.board_id}, exception in sending request with action_type: '{action_type}' to board via mqtt:")

    def update_configs(self, configs: List):
        self.configures.clear()
        self.configures.extend(configs)

    def notify_event_alarm(self, event_alarms: list):
        for n in self.event_alarm_notifiers:
            n.notify(event_alarms)

    def add_items(self, items: List[TimelineItem]):
        # self.logger.debug("board: {} is adding TimelineItem(s)...  type: {}".format(self.board_id, items[0].type.name))
        self.check_person_session(items)

        self.items += items
        if len(self.items) % 5 == 0:
            self.__purge_items()

        # self.foward_object_to_board(items)
        # if len(items) == 1 and items[0].raw_data == '':
        #    return

        # ebik session
        self.ebik_session.feed(items)
        event_alarms = []
        for d in self.event_detectors:
            t0 = time.time()
            new_alarms = None
            processing_items = None
            if d.is_session_detector:
                processing_items = items
            elif d.get_timeline_item_filter():
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
                    "board: {}, {} detect used time(ms): {}".format(self.board_id, d.__class__.__name__,
                                                                    perf_time_used_by_ms))

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
                    (datetime.datetime.now(datetime.timezone.utc) -
                     item.original_timestamp)
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

    # items为新加入列表的项
    def check_person_session(self, items: List[TimelineItem]):
        person_items = [item for item in items if
                        item.item_type == TimelineItemType.OBJECT_DETECT and "Person|#" in item.raw_data]
        object_items = [item for item in items if
                        item.item_type == TimelineItemType.OBJECT_DETECT]
        # 这一轮检测中有人
        if len(person_items) > 0:
            # 人在电梯里的session已经开始了，只需更新最近检测到人的时间
            if self.person_session["person_in"]:
                self.person_session["latest_person_item_time"] = datetime.datetime.now(
                    datetime.timezone.utc)
                self.person_session["person_count"] = len(person_items)
            else:
                self.person_session["person_in"] = True
                self.person_session["session_start_at"] = datetime.datetime.now(
                    datetime.timezone.utc)
                self.person_session["latest_person_item_time"] = datetime.datetime.now(
                    datetime.timezone.utc)
                self.person_session["person_count"] = len(person_items)
        elif len(object_items) > 0:
            # 这一轮上传中没有人，检查下最近一次检测到的人过去多长时间，超过4秒则认为没人
            if self.person_session["person_in"] and (datetime.datetime.now(datetime.timezone.utc) - self.person_session[
                    "latest_person_item_time"]).total_seconds() > 5:
                self.person_session["person_in"] = False
                self.person_session["session_start_at"] = None
                self.person_session["latest_person_item_time"] = None
                self.person_session["person_count"] = 0
