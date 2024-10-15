#!/usr/bin/env python
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#  * Neither the name of NVIDIA CORPORATION nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import atexit
import multiprocessing
from multiprocessing.connection import Connection
import random
import signal
import sys
import requests
from multiprocessing import Process
import tao_triton.python.device_hub.event_detectors.event_detector as event_detector
from tao_triton.python.device_hub.event_detectors.electric_bicycle_entering_event_detector import ElectricBicycleEnteringEventDetector
from tao_triton.python.device_hub.event_detectors.gas_tank_entering_event_detector import GasTankEnteringEventDetector
from tao_triton.python.device_hub.event_detectors.battery_entering_event_detector import BatteryEnteringEventDetector
import event_alarm
import board_timeline
from tao_triton.python.device_hub import board_timeline, util
from threading import Timer
import uuid
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import yaml
import logging.config
import logging
import base64_tao_client
from typing import List, Tuple, Union
import os
import time
import datetime
import argparse
import paho.mqtt.client as paho_mqtt_client
# infer_server_url = None
# infer_server_protocol = None
# infer_model_name = None
# infer_model_version = None
# infer_server_comm_output_verbose = None

with open('log_config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)


class RepeatTimer(Timer):
    def run(self):
        # run right now without waiting for the interval
        self.function(*self.args, **self.kwargs)
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


# shared_EventAlarmWebServiceNotifier = event_alarm.EventAlarmWebServiceNotifier(logging)


def create_boardtimeline(board_id: str, kafka_producer, mqtt_client: paho_mqtt_client, shared_EventAlarmWebServiceNotifier, target_borads: str, lift_id: str):
    if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
        event_detectors = [  # ElectricBicycleEnteringEventDetector(logging),
            # event_detector.DoorStateChangedEventDetector(logging),
            event_detector.DoorStateSessionDetector(logging),
            event_detector.ElectricBicycleEnteringEventDetector(logging),
            # GasTankEnteringEventDetector(logging),
            # BatteryEnteringEventDetector(logging),
            #    event_detector.PeopleStuckEventDetector(logging),
            #    # event_detector.GasTankEnteringEventDetector(logging),
            #    # event_detector.DoorOpenedForLongtimeEventDetector(logging),
            #    # event_detector.DoorRepeatlyOpenAndCloseEventDetector(logging),
            #    event_detector.ElevatorOverspeedEventDetector(logging),
            #    # event_detector.TemperatureTooHighEventDetector(logging),
            #    # event_detector.PassagerVigorousExerciseEventDetector(logging),
            #    # event_detector.DoorOpeningAtMovingEventDetector(logging),
            #    # event_detector.ElevatorSuddenlyStoppedEventDetector(logging),
            #    # event_detector.ElevatorShockEventDetector(logging),
            #    event_detector.ElevatorMovingWithoutPeopleInEventDetector(logging),
            #    event_detector.ElevatorJamsEventDetector(logging),
            #    event_detector.ElevatorMileageEventDetector(logging),
            #    event_detector.ElevatorRunningStateEventDetector(logging),
            #    event_detector.UpdateResultEventDetector(logging),
            #    event_detector.GyroscopeFaultEventDetector(logging),
            #    event_detector.PressureFaultEventDetector(logging),
            #    event_detector.ElectricSwitchFaultEventDetector(logging),
            #    event_detector.DeviceOfflineEventDetector(logging),
            #    event_detector.DetectPersonOnTopEventDetector(logging),
            #    event_detector.DetectCameraBlockedEventDetector(logging),
            # event_detector.CameraDetectVehicleEventDetector(logging)
        ]
        return board_timeline.BoardTimeline(logging, board_id, [],
                                            event_detectors,
                                            [event_alarm.EventAlarmDummyNotifier(
                                                logging)],
                                            kafka_producer, mqtt_client, target_borads, lift_id)
    # enable_developer_local_debug_mode
    # these detectors instances are shared by all timelines
    event_detectors = [ElectricBicycleEnteringEventDetector(logging),
                       event_detector.DoorStateSessionDetector(logging),
                       event_detector.RunningStateSessionDetector(logging),
                       event_detector.DoorStateChangedEventDetector(logging),
                       event_detector.BlockingDoorEventDetector(logging),
                       event_detector.PeopleStuckEventDetector(logging),
                       GasTankEnteringEventDetector(logging),
                       BatteryEnteringEventDetector(logging),
                       # event_detector.DoorOpenedForLongtimeEventDetector(logging),
                       # event_detector.DoorRepeatlyOpenAndCloseEventDetector(logging),
                       event_detector.ElevatorOverspeedEventDetector(logging),
                       # event_detector.TemperatureTooHighEventDetector(logging),
                       # event_detector.PassagerVigorousExerciseEventDetector(logging),
                       # event_detector.DoorOpeningAtMovingEventDetector(logging),
                       # event_detector.ElevatorSuddenlyStoppedEventDetector(logging),
                       # event_detector.ElevatorShockEventDetector(logging),
                       event_detector.ElevatorMovingWithoutPeopleInEventDetector(
                           logging),
                       event_detector.ElevatorJamsEventDetector(logging),
                       event_detector.ElevatorMileageEventDetector(logging),
                       event_detector.ElevatorRunningStateEventDetector(
                           logging),
                       event_detector.UpdateResultEventDetector(logging),
                       event_detector.GyroscopeFaultEventDetector(logging),
                       event_detector.PressureFaultEventDetector(logging),
                       event_detector.ElectricSwitchFaultEventDetector(
                           logging),
                       event_detector.DeviceOfflineEventDetector(logging),
                       event_detector.DetectPersonOnTopEventDetector(logging),
                       event_detector.DetectCameraBlockedEventDetector(
                           logging),
                       # event_detector.CameraDetectVehicleEventDetector(logging)
                       ]
    return board_timeline.BoardTimeline(logging, board_id, [],
                                        event_detectors,
                                        [  # event_alarm.EventAlarmDummyNotifier(logging),
                                            shared_EventAlarmWebServiceNotifier],
                                        kafka_producer, mqtt_client, target_borads, lift_id)


def pipe_in_local_idle_loop_item_to_board_timelines(board_timelines: list):
    if board_timelines:
        for tl in board_timelines:
            if tl.items:
                # a timeline wasn't fired for 5s, then pip in local idle loop item
                if (datetime.datetime.fromisoformat(datetime.datetime.now(
                        datetime.timezone.utc).astimezone().isoformat()) - tl.items[
                        -1].local_utc_timestamp).total_seconds() >= 5:
                    local_idle_loop_item = \
                        board_timeline.TimelineItem(tl, board_timeline.TimelineItemType.LOCAL_IDLE_LOOP,
                                                    datetime.datetime.now(
                                                        datetime.timezone.utc).astimezone().isoformat(),
                                                    str(uuid.uuid4()), "")
                    tl.add_items([local_idle_loop_item])


def get_configurations(board_timelines: list, logger):
    try:
        get_config = requests.get("https://api.glfiot.com/api/apiduijie/gettermnodes",
                                  headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        if get_config.status_code != 200:
            return
        json_result = get_config.json()
        valueable_config = []
        if "data" in json_result:
            for index, value in enumerate(json_result["data"]):
                if value["code"] == "krsj" or value["code"] == "csjkm" or value["code"] == "ffkgm" or value["code"] == "mbyc" or \
                        value["code"] == "kqzt" or value["code"] == "dtyd" or value["code"] == "tlygz" or value["code"] == "sblx" or \
                        value["code"] == "zdm" or value["code"] == "qyjgz" or value["code"] == "gbzt" or value["code"] == "mqgbzt":
                    valueable_config.append(value)
        for tl in board_timelines:
            tl.update_configs(valueable_config)
    except Exception as e:
        logger.exception(
            "device_hub, get_configurations error caused by exception:")
        return


def is_time_diff_too_big(board_id: str, boardMsgTimeStampStr: str, kafkaServerAppliedTimeStamp: int,
                         dh_local_datetime: datetime.datetime, logger: logging.Logger):
    """
    caculate the time difference between boardMsgTimeStamp, kafkaMsgTimeStamp and dh local datetime.
    :param board_id: the board id, used for logging.
    :param boardMsgTimeStampStr: the timestamp in board message, UTC datetime string, like: '2023-06-28T12:58:58.960Z'
    :param kafkaMsgTimeStamp: the kafka server labeled a int value of timestamp to all incoming messages.
    :param dh_local_datetime: the local datetime of device hub, it's the datetime.datetime.now()
    :return: the time difference in seconds
    """
    board_timestamp_utc_datetime = datetime.datetime.fromisoformat(
        boardMsgTimeStampStr.replace("Z", "+00:00"))
    # event.timestamp is a simple local(utc+8) datetime, like datetime.datetime(2023, 6, 28, 20, 58, 58, 995000)
    kafka_server_received_msg_utc_datetime = datetime.datetime.fromtimestamp(
        kafkaServerAppliedTimeStamp / 1e3).astimezone(board_timestamp_utc_datetime.tzinfo)
    dh_local_utc_datetime = dh_local_datetime.astimezone(
        board_timestamp_utc_datetime.tzinfo)
    
    Drop_Threshold = 8
    Warning_Threshold = 2.5
   
    time_diff_by_sub_kafka_to_board = (kafka_server_received_msg_utc_datetime
                                       - board_timestamp_utc_datetime
                                       ).total_seconds()
    if abs(time_diff_by_sub_kafka_to_board) < Drop_Threshold and abs(time_diff_by_sub_kafka_to_board) >= Warning_Threshold:
        logger.info(
            f"time_diff (kafka - board) is big: {time_diff_by_sub_kafka_to_board}s for board: {board_id}, board network upload slow or un-synced datetime?")
    if abs(time_diff_by_sub_kafka_to_board) >= Drop_Threshold:
        # log every 10 seconds for avoid log flooding
        # if datetime.datetime.now().second % 10 == 0:
        logger.warning(
            f"time_diff (kafka - board) is too big: {time_diff_by_sub_kafka_to_board}s for board: {board_id}, will drop msg, board network upload slow or un-synced datetime?")
        return True
    
    time_diff_by_sub_dh_to_kafka = (dh_local_utc_datetime
                                    - kafka_server_received_msg_utc_datetime
                                    ).total_seconds()
    if abs(time_diff_by_sub_dh_to_kafka) < Drop_Threshold and abs(time_diff_by_sub_dh_to_kafka) >= Warning_Threshold:
        logger.info(
            f"time_diff (dh_local - kafka) is big: {time_diff_by_sub_dh_to_kafka}s for board: {board_id}, dh process slow?")
    if abs(time_diff_by_sub_dh_to_kafka) >= Drop_Threshold:
        # log every 10 seconds for avoid log flooding
        # if datetime.datetime.now().second % 10 == 0:
        logger.warning(
            f"time_diff (dh_local - kafka) is too big: {time_diff_by_sub_dh_to_kafka}s for board: {board_id}, will drop msg, dh process slow?")
        return True
    return False


def split_array_to_group_of_chunks(arr: List[any], group_count: int):
    import math
    chunk_size = math.ceil(len(arr)/group_count)
    return [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]


def get_and_close_alarms():
    get_all_open_alarms = requests.get("https://api.glfiot.com/yunwei/warning/getopenliftwarning",
                                       headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    if get_all_open_alarms.status_code != 200:
        return ""
    json_result1 = get_all_open_alarms.json()
    if "result" in json_result1:
        for index, value in enumerate(json_result1["result"]):
            put_response = requests.put("https://api.glfiot.com/yunwei/warning/updatewarningmessagestatus",
                                        headers={
                                            'Content-type': 'application/json', 'Accept': 'application/json'},
                                        json={"liftId": value["liftId"],
                                              "type": value["type"],
                                              "warningMessageId": "",
                                              "base64string": ""})
            if put_response.status_code != 200 or put_response.json()["code"] != 200:
                main_logger.debug("failed to close lift:{} alarm: {}".format(
                    value["liftId"], value["name"]))
    return ""


def close_open_alarms(lift_ids: List[str]):
    """
    @param lift_ids: a list of lift id
    """
    all_open_alarms_response = requests.get("https://api.glfiot.com/yunwei/warning/getopenliftwarning",
                                            headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    if all_open_alarms_response.status_code != 200:
        return ""
    all_open_alarms_json_result = all_open_alarms_response.json()
    open_alarms_on_target_lifts = [
        alarm for alarm in all_open_alarms_json_result["result"] if alarm["liftId"] in lift_ids]
    for open_alarm in open_alarms_on_target_lifts:
        close_alarm_response = requests.put("https://api.glfiot.com/yunwei/warning/updatewarningmessagestatus",
                                            headers={
                                                'Content-type': 'application/json', 'Accept': 'application/json'},
                                            json={"liftId": open_alarm["liftId"],
                                                  "type": open_alarm["type"],
                                                  "warningMessageId": "",
                                                  "base64string": ""})
        if close_alarm_response.status_code != 200 or close_alarm_response.json()["code"] != 200:
            main_logger.debug("failed to close lift:{} alarm: {}".format(
                open_alarm["liftId"], open_alarm["name"]))


def get_xiaoquids(xiaoqu_name: str):
    get_all_board_ids_response = requests.get("https://api.glfiot.com/edge/all?xiaoquName="+xiaoqu_name,
                                              headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    if get_all_board_ids_response.status_code != 200:
        return ""

    json_result = get_all_board_ids_response.json()
    if "result" in json_result:
        target_boards = ""
        for index, value in enumerate(json_result["result"]):
            if index == 0:
                target_boards += value["serialNo"]
            else:
                target_boards += "|" + value["serialNo"]
        return target_boards
    return ""


def resolve_board_ids_from_board_definitions(board_definitions: list[dict]) -> list[str]:
    topics: list[str] = []
    for index, value in enumerate(board_definitions):
        board_id = value["serialNo"]
        if board_id == "E1640262214042521601" or board_id == "default_empty_id_please_manual_set_rv1126":
            continue
        topics.append(board_id)
    return topics


def setup_mqtt_client(board_definitions: list,
                      board_timelines: list[board_timeline.BoardTimeline],
                      process_name: str, logger: logging.Logger) -> paho_mqtt_client.Client:
    # Begin setup mqtt related
    mqtt_broker_url = "mqtt0.glfiot.com"
    mqtt_broker_port = 1883
    mqtt_client_id = f"devicehub_sub_process_{process_name}_id_{random.randint(1, 100000)}"
    mqtt_client = paho_mqtt_client.Client(
        paho_mqtt_client.CallbackAPIVersion.VERSION2, mqtt_client_id)
    mqtt_client.username_pw_set("devicehub_process", "devicehub_process")

    def on_mqtt_client_connect(client: paho_mqtt_client.Client, userdata, flags, rc, properties):
        if rc == 0 and client.is_connected():
            logger.info(
                f"process: {process_name} mqtt client connected to broker: {mqtt_broker_url}")
            board_ids = resolve_board_ids_from_board_definitions(
                board_definitions)
            log_str = " ".join(board_ids)
            logger.debug(
                f"mqtt client is subscribing with board_ids(count: {len(board_ids)}): {log_str}")
            for board_id in board_ids:
                mqtt_board_app_elenet_outbox_topic = f"board/{board_id}/elenet/outbox"
                client.subscribe(mqtt_board_app_elenet_outbox_topic)

    def on_mqtt_client_disconnect(client, userdata, disconnectflags, rc, properties):
        logger.error(
            f"process: {process_name} mqtt client disconnected from broker: {mqtt_broker_url}")

    def on_mqtt_message(client, userdata, message):
        board_id = message.topic.split("/")[1]
        boardtimeline = [
            b for b in board_timelines if b.board_id == board_id]
        if boardtimeline:
            boardtimeline[0].on_mqtt_message_from_board_outbox(
                client, userdata, message)
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.on_connect = on_mqtt_client_connect
    mqtt_client.on_disconnect = on_mqtt_client_disconnect
    mqtt_client.connect_async(mqtt_broker_url, mqtt_broker_port)
    mqtt_client.loop_start()
    time.sleep(2)
    while not GLOBAL_SHOULD_QUIT_EVERYTHING:
        try:
            if not mqtt_client.is_connected():
                print(
                    f"process: {process_name} mqtt client is not connected to broker: {mqtt_broker_url}, will block and keep retrying...")
                logger.error(
                    f"process: {process_name} mqtt client is not connected to broker: {mqtt_broker_url}, will block and keep retrying...")
                time.sleep(15)
                continue
            break
        except:
            print(
                f"process: {process_name} mqtt client exceptioned to connect to broker: {mqtt_broker_url}, will block and keep retrying...")
            logger.error(
                f"process: {process_name} mqtt client exceptioned to connect to broker: {mqtt_broker_url}, will block and keep retrying...")
            time.sleep(15)
            continue
    return mqtt_client
    # End setup mqtt related


def worker_of_process_board_msg(board_definitions: list[dict], process_name: str, target_borads: str, conn: Connection):
    """
    @param board_definitions: the board definitions that had been loaded from web service: [{"serialNo": i['serialNo'], "liftId": i['liftId']}]
    """
    with open('log_config.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        file_handlers = [config['handlers'][handler_name]
                         for handler_name in config['handlers'] if 'file_handler' in handler_name]
        for h in file_handlers:
            h['filename'] = h['filename'].replace(
                'log/', 'log/'+process_name+"/")
            per_process_log_folder = os.path.dirname(h['filename'])
            if not os.path.exists(per_process_log_folder):
                os.makedirs(per_process_log_folder)
        logging.config.dictConfig(config)

    global GLOBAL_SHOULD_QUIT_EVERYTHING

    per_process_main_logger = logging.getLogger()
    per_process_perf_logger = logging.getLogger("perfLogger")
    per_process_main_logger.info(
        'process: {} is running...'.format(process_name))

    board_timelines: list[board_timeline.BoardTimeline] = []
    timely_pipe_in_local_idle_loop_msg_timer = RepeatTimer(
        2, pipe_in_local_idle_loop_item_to_board_timelines, [board_timelines])
    timely_pipe_in_local_idle_loop_msg_timer.start()
    timely_get_config_timer = RepeatTimer(
        600, get_configurations, [board_timelines, per_process_main_logger])
    timely_get_config_timer.start()
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=FLAGS.kafka_server_url,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=str(uuid.uuid1()),
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    eventAlarmWebServiceNotifier = event_alarm.EventAlarmWebServiceNotifier(
        logging)
    kafka_producer = KafkaProducer(bootstrap_servers=FLAGS.kafka_server_url,
                                   value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    mqtt_client = setup_mqtt_client(
        board_definitions, board_timelines, process_name, per_process_main_logger)

    board_ids: list[str] = resolve_board_ids_from_board_definitions(
        board_definitions)
    kafka_consumer.subscribe(topics=board_ids)
    # at 2024.07.26, each board has 2 messages/s incoming here via kafka.
    # if future has different frequency, this should be adjusted.
    PERF_proximity_of_one_board_uploading_msg_count_per_second = 2
    # let's define the MAX delay time for the whole boards queue is 2 seconds.
    # then each msg should be processed within this time:
    PERF_healthy_avg_process_time_for_one_msg = 2 * \
        1000 / (len(board_ids) *
                PERF_proximity_of_one_board_uploading_msg_count_per_second)
    # set it to every 10 seconds to look back and caculate avg process time for one msg
    PERF_watch_process_time_for_one_msg_window_size = (
        len(board_ids)*PERF_proximity_of_one_board_uploading_msg_count_per_second)*10
    PERF_watch_process_time_for_one_msg_window: List[tuple] = []
    while not GLOBAL_SHOULD_QUIT_EVERYTHING:
        try:
            log_str = " ".join(board_ids)
            per_process_main_logger.debug(
                f"kafka_consumer is polling with topics(count: {len(board_ids)}): {log_str}")
            for event in kafka_consumer:
                if GLOBAL_SHOULD_QUIT_EVERYTHING:
                    break
                perf_counter_worker_start_time = time.time()

                if datetime.datetime.now().second % 5 == 0 and conn.poll():
                    msg: Union[list[dict[str, any]], str] = conn.recv()
                    if isinstance(msg, str):
                        if msg.startswith("command:exit"):
                            per_process_main_logger.critical(
                                f"process: {process_name} received exit command: {msg}")
                            GLOBAL_SHOULD_QUIT_EVERYTHING = True
                            break
                    else:
                        per_process_main_logger.info(
                            f"received incremental board_definitions msg: {msg}")
                        per_process_main_logger.info(
                            f"will extend board_definitions(previous count: {len(board_definitions)}): +{len(msg)}")
                        board_definitions.extend(msg)
                        board_ids: list[str] = resolve_board_ids_from_board_definitions(
                            board_definitions)
                        kafka_consumer.unsubscribe()
                        kafka_consumer.subscribe(topics=board_ids)

                        incremental_board_ids = resolve_board_ids_from_board_definitions(
                            msg)
                        for b in incremental_board_ids:
                            mqtt_client.subscribe(f"board/{b}/elenet/outbox")
                        per_process_main_logger.info(
                            f"mqtt client incremental subscribed times: {len(incremental_board_ids)}")
                        break

                event_data = event.value
                if "sensorId" not in event_data or "@timestamp" not in event_data:
                    continue
                board_msg_id = event_data["id"]
                # UTC datetime str, like: '2023-06-28T12:58:58.960Z'
                board_msg_original_timestamp = event_data["@timestamp"]
                board_id = event_data["sensorId"]

                if is_time_diff_too_big(board_id, board_msg_original_timestamp, event.timestamp, datetime.datetime.now(),
                                        per_process_perf_logger):
                    continue

                cur_board_timeline = [t for t in board_timelines if
                                      t.board_id == board_id]
                if not cur_board_timeline:
                    board_lift = [
                        b for b in board_definitions if b["serialNo"] == board_id]
                    lift_id = "" if not board_lift else board_lift[0]["liftId"]
                    cur_board_timeline = create_boardtimeline(board_id, kafka_producer, mqtt_client,
                                                              eventAlarmWebServiceNotifier, target_borads, lift_id)
                    board_timelines.append(cur_board_timeline)
                else:
                    cur_board_timeline = cur_board_timeline[0]
                new_timeline_items = []
                # indicates it's the object detection msg
                if "objects" in event_data:
                    is_frame_grayscale = False if not "is_frame_grayscale" in event_data else event_data[
                        "is_frame_grayscale"]
                    version = False if not "version" in event_data else event_data["version"]
                    if len(event_data["objects"]) == 0:
                        new_timeline_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.OBJECT_DETECT,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, "", is_frame_grayscale, version))
                    for obj_data in event_data["objects"]:
                        new_timeline_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.OBJECT_DETECT,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, obj_data, is_frame_grayscale, version))
                    cur_board_timeline.add_items(new_timeline_items)
                # indicates it's the sensor data reading msg
                elif "sensors" in event_data and "sensorId" in event_data:
                    # new_items = []
                    for obj_data in event_data["sensors"]:
                        if "speed" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_SPEED,
                                                            board_msg_original_timestamp,
                                                            board_msg_id, obj_data))
                            # cur_board_timeline.add_items([new_timeline_item])
                        elif "pressure" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_PRESSURE,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                            # cur_board_timeline.add_items([new_timeline_item])
                        elif "ACCELERATOR" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_ACCELERATOR,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                            # cur_board_timeline.add_items([new_timeline_item])
                        elif "switchFault" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_ELECTRIC_SWITCH,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                        elif "detectPerson" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                        elif "cameraBlocked" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.CAMERA_BLOCKED,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                        elif "eventType" in obj_data:
                            new_timeline_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.CAMERA_VECHILE_EVENT,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                    cur_board_timeline.add_items(new_timeline_items)
                elif "update" in event_data:
                    new_update_timeline_items = [board_timeline.TimelineItem(cur_board_timeline,
                                                                             board_timeline.TimelineItemType.UPDATE_RESULT,
                                                                             board_msg_original_timestamp,
                                                                             board_msg_id,
                                                                             event_data)]
                    new_timeline_items.append(new_timeline_items)
                    cur_board_timeline.add_items(new_update_timeline_items)

                perf_one_message_process_time_by_ms = (
                    time.time() - perf_counter_worker_start_time) * 1000
                PERF_watch_process_time_for_one_msg_window.append(
                    (perf_one_message_process_time_by_ms, event_data))
                # it's time to look back and check the perf statistics
                if len(PERF_watch_process_time_for_one_msg_window) > PERF_watch_process_time_for_one_msg_window_size:
                    sum_of_process_time = sum(
                        tp[0] for tp in PERF_watch_process_time_for_one_msg_window)
                    avg_of_process_time = sum_of_process_time / \
                        len(PERF_watch_process_time_for_one_msg_window)
                    if avg_of_process_time >= PERF_healthy_avg_process_time_for_one_msg:
                        tuple_of_max_time = max(
                            PERF_watch_process_time_for_one_msg_window, key=lambda x: x[0])
                        per_process_perf_logger.info(
                            f"perf low, healthy avg process time for one msg is: {PERF_healthy_avg_process_time_for_one_msg}ms, "
                            f"but in last period which is: {avg_of_process_time}ms, \n"
                            f"and the max time-consuming is: {tuple_of_max_time[0]}ms, msg is: {tuple_of_max_time[1]}")
                    PERF_watch_process_time_for_one_msg_window = []
        except Exception as e:
            per_process_main_logger.exception(
                "Major error caused by exception:")
            print(e)
            continue

    try:
        per_process_main_logger.critical(
            "process: {} is exiting...".format(process_name))
        timely_pipe_in_local_idle_loop_msg_timer.cancel()
        timely_get_config_timer.cancel()

        timely_pipe_in_local_idle_loop_msg_timer.join()
        timely_get_config_timer.join()

        kafka_consumer.close()
        kafka_producer.close()

        mqtt_client.loop_stop()
        mqtt_client.disconnect()

        per_process_main_logger.critical(
            "process: {} exited...".format(process_name))
        print("process: {} exited...".format(process_name))
    except Exception as e:
        per_process_main_logger.exception(
            "Major error caused by exception in exit:")
        print(e)
        return


def check_and_update_incremental_board_info_via_web_service_and_send_msg_to_process_via_conn(
        running_processes: List[Process],
        parent_and_child_connections: List[Tuple[Connection, Connection]]):
    """
    @param previous_board_infos: the board infos that had been handled in all processes.
    sample:
    [{"id": "99994085712737341441","serialNo":"E1782703991956705305",
        "liftId":"99994085712737341441"}]
    """
    global All_Board_Infos
    try:
        get_all_board_ids_response = requests.get("https://api.glfiot.com/edge/all",
                                                  headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        if get_all_board_ids_response.status_code != 200:
            main_logger.error("re-get all board ids failed, status code: {}".format(
                str(get_all_board_ids_response.status_code)))
            print("re get all board ids  failed, status code: {}".format(
                str(get_all_board_ids_response.status_code)))
            return
        if "result" not in get_all_board_ids_response.json():
            main_logger.error(
                "re get all board ids  failed, result not in response")
            print("re get all board ids  failed, result not in response")
            return
        latest_all_board_infos = get_all_board_ids_response.json()["result"]
        incremental = [
            b for b in latest_all_board_infos if b["serialNo"] not in [p["serialNo"] for p in All_Board_Infos]]
        if incremental:
            # a simple balance strategy, select a process randomly to send the msg for loading the new boards
            selected_process_index = datetime.datetime.now().second % len(running_processes)
            main_logger.info("detected incremental board info, will load to process {}, incremental info count: {}: {}".format(
                selected_process_index, len(incremental), ', '.join([i['serialNo'] for i in incremental])))
            # incremental_serialNo_str = "|".join(
            #     [i['serialNo'] for i in incremental])
            incremental_serialNo_and_liftId_info = [
                {"id": i['liftId'], "serialNo": i['serialNo'], "liftId": i['liftId']} for i in incremental]
            parent_and_child_connections[selected_process_index][0].send(
                incremental_serialNo_and_liftId_info)
            All_Board_Infos = latest_all_board_infos

    except Exception as e:
        main_logger.exception(
            "check_and_update_incremental_board_info_via_web_service_and_send_msg_to_process_via_conn error caused by exception:")
        return


GLOBAL_CONCURRENT_PROCESS_COUNT = 40

# a global flag to indicate whether all processes should exit
GLOBAL_SHOULD_QUIT_EVERYTHING = False

# all loaded (by processes) board infos from web service
# sample:
# [{"id": "99994085712737341441","serialNo":"E1782703991956705305","liftId":"99994085712737341441"}]
All_Board_Infos: List[any]


def exit_handler():
    # print("Cleaning up before exit via exit_handler...")
    global GLOBAL_SHOULD_QUIT_EVERYTHING
    GLOBAL_SHOULD_QUIT_EVERYTHING = True


def kill_handler(*args):
    global GLOBAL_SHOULD_QUIT_EVERYTHING
    GLOBAL_SHOULD_QUIT_EVERYTHING = True


atexit.register(exit_handler)
signal.signal(signal.SIGINT, kill_handler)
signal.signal(signal.SIGTERM, kill_handler)

if __name__ == '__main__':
    main_logger = logging.getLogger()
    main_logger.info('%s is starting...', 'device_hub')
    parser = argparse.ArgumentParser()
    parser.add_argument('-v',
                        '--verbose',
                        action="store_true",
                        required=False,
                        default=False,
                        help='Enable verbose output')
    parser.add_argument('-a',
                        '--async',
                        dest="async_set",
                        action="store_true",
                        required=False,
                        default=False,
                        help='Use asynchronous inference API')
    parser.add_argument('--streaming',
                        action="store_true",
                        required=False,
                        default=False,
                        help='Use streaming inference API. ' +
                             'The flag is only available with gRPC protocol.')
    parser.add_argument('-m',
                        '--model-name',
                        type=str,
                        required=False,
                        default="bicycletypenet_tao",
                        help='Name of model')
    parser.add_argument('-x',
                        '--model-version',
                        type=str,
                        required=False,
                        default="",
                        help='Version of model. Default is to use latest version.')
    parser.add_argument('-b',
                        '--batch-size',
                        type=int,
                        required=False,
                        default=1,
                        help='Batch size. Default is 1.')
    parser.add_argument('--mode',
                        type=str,
                        choices=['Classification', "DetectNet_v2", "LPRNet", "YOLOv3", "Peoplesegnet", "Retinanet",
                                 "Multitask_classification"],
                        required=False,
                        default='Classification',
                        help='Type of scaling to apply to image pixels. Default is NONE.')
    parser.add_argument('-u',
                        '--url',
                        type=str,
                        required=False,
                        default='localhost:8000',
                        help='Inference server URL. Default is localhost:8000.')
    parser.add_argument('-i',
                        '--protocol',
                        type=str,
                        required=False,
                        default='HTTP',
                        help='Protocol (HTTP/gRPC) used to communicate with ' +
                             'the inference service. Default is HTTP.')
    # parser.add_argument('image_filename',
    #                     type=str,
    #                     nargs='?',
    #                     default=None,
    #                     help='Input image / Input folder')
    parser.add_argument('--class_list',
                        type=str,
                        default="bicycle,electric_bicycle",
                        help="Comma separated class names",
                        required=False)
    parser.add_argument('--output_path',
                        type=str,
                        default=os.path.join(os.getcwd(), "outputs"),
                        help="Path to where the inferenced outputs are stored.",
                        required=False)
    parser.add_argument("--postprocessing_config",
                        type=str,
                        default="",
                        help="Path to the DetectNet_v2 clustering config.")
    parser.add_argument('--kafka-server-url',
                        type=str,
                        required=False,
                        default='msg.glfiot.com:9092',
                        help='kafka server URL. Default is xxx:9092.')
    FLAGS = parser.parse_args()

    get_and_close_alarms()

    concurrent_processes: List[Process] = []
    parent_and_child_connections: List[Tuple[Connection, Connection]] = []

    get_all_board_infos_response = requests.get("https://api.glfiot.com/edge/all",
                                                headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    if get_all_board_infos_response.status_code != 200:
        main_logger.error("get all board infos failed, status code: {}".format(
            str(get_all_board_infos_response.status_code)))
        print("get all board infos failed, status code: {}".format(
            str(get_all_board_infos_response.status_code)))
        exit(1)
    if "result" not in get_all_board_infos_response.json():
        main_logger.error("get all board infos failed, result not in response")
        print("get all board infos failed, result not in response")
        exit(1)

    # sample:
    # [{"id": "99994085712737341441","serialNo":"E1782703991956705305","liftId":"99994085712737341441"}]
    All_Board_Infos = get_all_board_infos_response.json()["result"]
    if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
        GLOBAL_CONCURRENT_PROCESS_COUNT = 1
        All_Board_Infos = All_Board_Infos[200:1000]
    # duration is in seconds
    timely_check_incremental_board_info_timer = RepeatTimer(
        30, check_and_update_incremental_board_info_via_web_service_and_send_msg_to_process_via_conn,
        [concurrent_processes, parent_and_child_connections])
    if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
        pass
    else:
        timely_check_incremental_board_info_timer.start()

    # below is a sample for local DEBUG use
    # All_Board_Infos = [{"id": "99994085712737341441",
    #                            "serialNo": "E1782703991956705305", "liftId": "99994085712737341441"}]
    main_logger.info("total board info count from web service is: {}".format(
        str(len(All_Board_Infos))))
    print("total board info count from web service is: {}".format(
        str(len(All_Board_Infos))))
    board_info_chunks = split_array_to_group_of_chunks(
        All_Board_Infos, GLOBAL_CONCURRENT_PROCESS_COUNT)
    chunk_index = 0
    # target_borads = get_xiaoquids("心泊家园（梅花苑）")
    # target_borads = target_borads + get_xiaoquids("江南平安里")

    for ck in board_info_chunks:
        process_name = str(chunk_index)
        main_logger.info("process: {}, board count assigned: {}, they're: {}".format(
            process_name, len(ck), ','.join([i['serialNo'] for i in ck])))
        print("process: {}, board count assigned: {}".format(
            process_name, len(ck)))
        parent_conn, child_conn = multiprocessing.Pipe()
        parent_and_child_connections.append((parent_conn, child_conn))
        p = Process(target=worker_of_process_board_msg,
                    args=(ck, process_name, "", child_conn))
        concurrent_processes.append(p)
        p.start()
        print("process: {} started".format(process_name))
        chunk_index += 1

    while not GLOBAL_SHOULD_QUIT_EVERYTHING:
        time.sleep(1)
    timely_check_incremental_board_info_timer.cancel()
    print("all processes are exiting, will wait for all processes to exit...")
    main_logger.critical(
        "all processes are exiting, will wait for all processes to exit...")
    time.sleep(5)
    for p in concurrent_processes:
        main_logger.critical(
            "process: {} waiting for exit...".format(p.name))
        print("process: {} waiting for exit...".format(p.name))
        p.join()
        main_logger.critical(
            "process: {} join done".format(p.name))
        print("     process: {} is_alive: {}".format(p.name, p.is_alive()))
        print("     process: {} join done".format(p.name))
        # print("     process: {} join done".format(p.name))

    print("all processes are exited successfully.")
    main_logger.critical("all processes are exited successfully.")
