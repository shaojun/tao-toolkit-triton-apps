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
import signal
import sys
import requests
from multiprocessing import Process
import event_detector
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
from typing import List
import os
import time
import datetime
import argparse

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
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


# shared_EventAlarmWebServiceNotifier = event_alarm.EventAlarmWebServiceNotifier(logging)


def create_boardtimeline(board_id: str, kafka_producer, shared_EventAlarmWebServiceNotifier, target_borads: str, lift_id: str):
    if util.read_fast_from_app_config_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
        event_detectors = [event_detector.ElectricBicycleEnteringEventDetector(logging),
                           #    event_detector.DoorStateChangedEventDetector(logging),
                           #    event_detector.BlockingDoorEventDetector(logging),
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
                                            [event_alarm.EventAlarmDummyNotifier(logging),
                                             event_detector.DeviceOfflineEventDetector(
                                                logging)
                                             ],
                                            kafka_producer, target_borads, lift_id)
    # enable_developer_local_debug_mode
    # these detectors instances are shared by all timelines
    event_detectors = [event_detector.ElectricBicycleEnteringEventDetector(logging),
                       event_detector.DoorStateChangedEventDetector(logging),
                       event_detector.BlockingDoorEventDetector(logging),
                       event_detector.PeopleStuckEventDetector(logging),
                       event_detector.GasTankEnteringEventDetector(logging),
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
                                        kafka_producer, target_borads, lift_id)


def logging_perf_counter(process_name: str):
    global PERF_COUNTER_consumed_msg_count
    global PERF_COUNTER_filtered_msg_count_by_time_diff_too_big
    global PERF_COUNTER_work_time_by_ms
    perf_logger = logging.getLogger("perfLogger")
    perf_logger.warning("process: {}, consumed_msg_count: {}".format(
        process_name, PERF_COUNTER_consumed_msg_count))
    perf_logger.warning("process: {}, filtered_msg_count_by_time_diff_too_big: {}".format(
        process_name, PERF_COUNTER_filtered_msg_count_by_time_diff_too_big))
    perf_logger.warning("process: {}, work_time_by_ms: {}".format(
        process_name, PERF_COUNTER_work_time_by_ms))
    # reset it
    PERF_COUNTER_consumed_msg_count = 0
    PERF_COUNTER_filtered_msg_count_by_time_diff_too_big = 0
    PERF_COUNTER_work_time_by_ms = 0


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


def get_configurations(board_timelines: list):
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
                    value["code"] == "zdm" or value["code"] == "qyjgz":
                valueable_config.append(value)
    for tl in board_timelines:
        tl.update_configs(valueable_config)


def is_time_diff_too_big(board_id: str, boardMsgTimeStampStr: str, kafkaServerAppliedTimeStamp: int,
                         dh_local_datetime: datetime.datetime):
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
    time_diff_between_board_and_kafka = (board_timestamp_utc_datetime -
                                         kafka_server_received_msg_utc_datetime).total_seconds()
    if abs(time_diff_between_board_and_kafka) >= 10:
        # log every 10 seconds for avoid log flooding
        if datetime.datetime.now().second % 10 == 0:
            logging.warning("time_diff between board and kafka is too big: %s for board with id: %s",
                            time_diff_between_board_and_kafka, board_id)
        return True
    time_diff_between_kafka_and_dh_local = (kafka_server_received_msg_utc_datetime -
                                            dh_local_utc_datetime).total_seconds()
    if abs(time_diff_between_kafka_and_dh_local) >= 4:
        # log every 10 seconds for avoid log flooding
        if datetime.datetime.now().second % 10 == 0:
            logging.warning("time_diff between kafka and dh_local is too big: %s for board with id: %s",
                            time_diff_between_kafka_and_dh_local, board_id)
        return True
    return False


def split_array_to_group_of_chunks(arr, group_count: int):
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


def worker_of_process_board_msg(boards: List, process_name: str, target_borads: str):
    with open('log_config.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        file_handlers = [config['handlers'][handler_name]
                         for handler_name in config['handlers'] if 'file_handler' in handler_name]
        for h in file_handlers:
            h['filename'] = h['filename'].replace(
                'log/', 'log/'+process_name+"/")
            if not os.path.exists('log/'+process_name+"/"):
                os.makedirs('log/'+process_name+"/")
        logging.config.dictConfig(config)

    global PERF_COUNTER_consumed_msg_count
    global PERF_COUNTER_filtered_msg_count_by_time_diff_too_big
    global PERF_COUNTER_work_time_by_ms
    global GLOBAL_SHOULD_QUIT_EVERYTHING

    per_process_main_logger = logging.getLogger()
    per_process_main_logger.info(
        'process: {} is running...'.format(process_name))

    # duration is in seconds
    timely_logging_perf_counter_timer = RepeatTimer(
        PERF_LOGGING_INTERVAL, logging_perf_counter, [process_name])
    timely_logging_perf_counter_timer.start()

    board_timelines = []
    timely_pipe_in_local_idle_loop_msg_timer = RepeatTimer(
        2, pipe_in_local_idle_loop_item_to_board_timelines, [board_timelines])
    timely_pipe_in_local_idle_loop_msg_timer.start()
    timely_get_config_timer = RepeatTimer(
        600, get_configurations, [board_timelines])
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
    if boards == None:
        return
    consumer_str = ""
    for index, value in enumerate(boards):
        if index == 0:
            consumer_str += value["serialNo"]
        else:
            consumer_str += "|" + value["serialNo"]
    kafka_consumer.subscribe(pattern=consumer_str)
    while not GLOBAL_SHOULD_QUIT_EVERYTHING:
        try:
            # do a dummy poll to retrieve some message
            kafka_consumer.poll()
            per_process_main_logger.debug(
                "process: {}, consumer.poll messages".format(process_name))
            # go to end of the stream
            kafka_consumer.seek_to_end()
            for event in kafka_consumer:
                if GLOBAL_SHOULD_QUIT_EVERYTHING:
                    break
                event_data = event.value
                if "sensorId" not in event_data or "@timestamp" not in event_data:
                    continue
                board_msg_id = event_data["id"]
                # UTC datetime str, like: '2023-06-28T12:58:58.960Z'
                board_msg_original_timestamp = event_data["@timestamp"]
                board_id = event_data["sensorId"]
                if board_id == "default_empty_id_please_manual_set_rv1126":
                    continue

                # if board_id != "E1634085712737341441":
                #    continue

                if board_id == "E1640262214042521601":
                    continue

                if board_id == "E1675418684153139201":
                    continue

                if "_dh" in board_id:
                    continue

                if is_time_diff_too_big(board_id, board_msg_original_timestamp, event.timestamp, datetime.datetime.now()):
                    PERF_COUNTER_filtered_msg_count_by_time_diff_too_big += 1
                    continue
                perf_counter_work_time_start_time = time.time()
                cur_board_timeline = [t for t in board_timelines if
                                      t.board_id == board_id]
                if not cur_board_timeline:
                    board_lift = [
                        b for b in boards if b["serialNo"] == board_id]
                    lift_id = "" if not board_lift else board_lift[0]["liftId"]
                    cur_board_timeline = create_boardtimeline(board_id, kafka_producer,
                                                              eventAlarmWebServiceNotifier, target_borads, lift_id)
                    board_timelines.append(cur_board_timeline)
                else:
                    cur_board_timeline = cur_board_timeline[0]
                new_timeline_items = []
                # indicates it's the object detection msg
                if "objects" in event_data:
                    if len(event_data["objects"]) == 0:
                        new_timeline_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.OBJECT_DETECT,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, ""))
                    for obj_data in event_data["objects"]:
                        new_timeline_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.OBJECT_DETECT,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, obj_data))
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
                PERF_COUNTER_work_time_by_ms += (time.time() -
                                                 perf_counter_work_time_start_time) * 1000
                PERF_COUNTER_consumed_msg_count += 1
        except Exception as e:
            per_process_main_logger.exception(
                "Major error caused by exception:")
            print(e)
            continue

    try:
        per_process_main_logger.critical(
            "process: {} is exiting...".format(process_name))
        timely_logging_perf_counter_timer.cancel()
        timely_pipe_in_local_idle_loop_msg_timer.cancel()
        timely_get_config_timer.cancel()

        timely_logging_perf_counter_timer.join()
        timely_pipe_in_local_idle_loop_msg_timer.join()
        timely_get_config_timer.join()
        kafka_consumer.close()
        kafka_producer.close()
        per_process_main_logger.critical(
            "process: {} exited...".format(process_name))
        print("process: {} exited...".format(process_name))
    except Exception as e:
        per_process_main_logger.exception(
            "Major error caused by exception in exit:")
        print(e)
        return


'''each process owned below variables, they're not shared between processes'''
PERF_LOGGING_INTERVAL = 60
PERF_COUNTER_consumed_msg_count = 0
PERF_COUNTER_filtered_msg_count_by_time_diff_too_big = 0
PERF_COUNTER_work_time_by_ms = 0

GLOBAL_CONCURRENT_PROCESS_COUNT = 6

# a global flag to indicate whether all processes should exit
GLOBAL_SHOULD_QUIT_EVERYTHING = False


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

    concurrent_processes = []
    get_all_board_ids_response = requests.get("https://api.glfiot.com/edge/all",
                                              headers={'Content-type': 'application/json', 'Accept': 'application/json'})
    if get_all_board_ids_response.status_code != 200:
        main_logger.error("get all board ids  failed, status code: {}".format(
            str(get_all_board_ids_response.status_code)))
        print("get all board ids  failed, status code: {}".format(
            str(get_all_board_ids_response.status_code)))
        exit(1)
    json_result = get_all_board_ids_response.json()

    # below is a sample for local debug
    # json_result = {"result":[{"id": "99994085712737341441","serialNo":"Test_New_board","liftId":"99994085712737341441"}]}
    if "result" in json_result:
        total_board_count = len(json_result["result"])
        main_logger.info("total board count from web service is: {}".format(
            str(total_board_count)))
        print("total board count from web service is: {}".format(
            str(total_board_count)))
        board_info_chunks = split_array_to_group_of_chunks(
            json_result["result"], GLOBAL_CONCURRENT_PROCESS_COUNT)
        chunk_index = 0
        # target_borads = get_xiaoquids("心泊家园（梅花苑）")
        # target_borads = target_borads + get_xiaoquids("江南平安里")

        # below is a sample for local debug
        # target_borads="Test_New_board"
        for ck in board_info_chunks:
            process_name = str(chunk_index)
            main_logger.info("process: {}, board count assigned: {}, they're: {}".format(
                process_name, len(ck), ','.join([i['serialNo'] for i in ck])))
            print("process: {}, board count assigned: {}".format(
                process_name, len(ck)))
            p = Process(target=worker_of_process_board_msg,
                        args=(ck, process_name, ""))
            concurrent_processes.append(p)
            p.start()
            print("process: {} started".format(process_name))
            chunk_index += 1

        while not GLOBAL_SHOULD_QUIT_EVERYTHING:
            time.sleep(1)
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
    else:
        main_logger.error("get all board ids failed, result not in response")
        print("get all board ids failed, result not in response")
        exit(1)
