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

import argparse
import datetime
import time
import os
from typing import List

import base64_tao_client
import logging
import logging.config
import yaml
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import uuid
from threading import Timer
# infer_server_url = None
# infer_server_protocol = None
# infer_model_name = None
# infer_model_version = None
# infer_server_comm_output_verbose = None
from tao_triton.python.device_hub import board_timeline
import board_timeline
import event_alarm
import event_detector

with open('log_config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def create_boardtimeline(board_id: str, producer):
    # these detectors instances are shared by all timelines
    event_detectors = [event_detector.ElectricBicycleEnteringEventDetector(logging),
                       event_detector.DoorStateChangedEventDetector(logging),
                       event_detector.BlockingDoorEventDetector(logging),
                       event_detector.PeopleStuckEventDetector(logging),
                       event_detector.GasTankEnteringEventDetector(logging),
                       event_detector.DoorOpenedForLongtimeEventDetector(logging),
                       event_detector.DoorRepeatlyOpenAndCloseEventDetector(logging),
                       event_detector.ElevatorOverspeedEventDetector(logging),
                       # event_detector.TemperatureTooHighEventDetector(logging),
                       # event_detector.PassagerVigorousExerciseEventDetector(logging),
                       # event_detector.DoorOpeningAtMovingEventDetector(logging),
                       # event_detector.ElevatorSuddenlyStoppedEventDetector(logging),
                       # event_detector.ElevatorShockEventDetector(logging),
                       event_detector.ElevatorMovingWithoutPeopleInEventDetector(logging),
                       event_detector.ElevatorJamsEventDetector(logging),
                       event_detector.ElevatorMileageEventDetector(logging),
                       event_detector.ElevatorRunningStateEventDetector(logging),
                       event_detector.UpdateResultEventDetector(logging),
                       event_detector.GyroscopeFaultEventDetector(logging),
                       event_detector.PressureFaultEventDetector(logging),
                       event_detector.ElectricSwitchFaultEventDetector(logging),
                       event_detector.DeviceOfflineEventDetector(logging),
                       event_detector.DetectPersonOnTopEventDetector(logging),
                       event_detector.DetectCameraBlockedEventDetector(logging),
                       # event_detector.CameraDetectVehicleEventDetector(logging)
                       ]
    return board_timeline.BoardTimeline(logging, board_id, [],
                                        event_detectors,
                                        [
                                            # event_alarm.EventAlarmDummyNotifier(logging),
                                            event_alarm.EventAlarmWebServiceNotifier(
                                                logging)
    ],producer)


def create_boardtimeline_from_web_service() -> List[board_timeline.BoardTimeline]:
    return []


BOARD_TIMELINES = None


def pipe_in_local_idle_loop_item_to_board_timelines():
    if BOARD_TIMELINES:
        for tl in BOARD_TIMELINES:
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


def is_time_diff_too_big(board_id: str, boardMsgTimeStampStr: str, kafkaServerAppliedTimeStamp: int, dh_local_datetime: datetime.datetime):
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
    if abs(time_diff_between_kafka_and_dh_local) >= 2.5:
        # log every 10 seconds for avoid log flooding
        if datetime.datetime.now().second % 10 == 0:
            logging.warning("time_diff between kafka and dh_local is too big: %s for board with id: %s",
                            time_diff_between_kafka_and_dh_local, board_id)
        return True
    return False


# duration is in seconds
timely_pipe_in_local_idle_loop_msg_timer = RepeatTimer(
    2, pipe_in_local_idle_loop_item_to_board_timelines)
if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.info('%s is starting...', 'device_hub')

    BOARD_TIMELINES = create_boardtimeline_from_web_service()
    timely_pipe_in_local_idle_loop_msg_timer.start()
    # except Exception as e:
    # logging.error("Exception occurred", exc_info=True)
    # or logging.exception("descriptive msg")  trace will be autoly appended
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

    consumer = KafkaConsumer(
        bootstrap_servers=FLAGS.kafka_server_url,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=str(uuid.uuid1()),
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    # consumer.subscribe(pattern="1423820088517")
    # consumer.subscribe(pattern="shaoLocalJsNxBoard")
    # consumer.subscribe(pattern="E1630452176113373185")
    # consumer.subscribe(pattern="^E[0-9]+$")
    # consumer.subscribe(pattern="shaoLocalJts2gBoard")
    # consumer.subscribe(pattern="^(E163|E160)[0-9]+$")
    consumer.subscribe(pattern="^(E16)[0-9]+$")

while True:
    try:
        # do a dummy poll to retrieve some message
        consumer.poll()
        logger.debug("consumer.poll messages")
        # go to end of the stream
        consumer.seek_to_end()
        for event in consumer:
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
                continue

            cur_board_timeline = [t for t in BOARD_TIMELINES if
                                  t.board_id == board_id]
            if not cur_board_timeline:
                cur_board_timeline = create_boardtimeline(board_id, producer)
                BOARD_TIMELINES.append(cur_board_timeline)
            else:
                cur_board_timeline = cur_board_timeline[0]
            # indicates it's the object detection msg
            if "objects" in event_data:
                new_timeline_items = []
                if len(event_data["objects"]) == 0:
                    new_timeline_items.append(
                        board_timeline.TimelineItem(cur_board_timeline, board_timeline.TimelineItemType.OBJECT_DETECT,
                                                    board_msg_original_timestamp,
                                                    board_msg_id, ""))
                for obj_data in event_data["objects"]:
                    new_timeline_items.append(
                        board_timeline.TimelineItem(cur_board_timeline, board_timeline.TimelineItemType.OBJECT_DETECT,
                                                    board_msg_original_timestamp,
                                                    board_msg_id, obj_data))
                cur_board_timeline.add_items(new_timeline_items)

            # indicates it's the sensor data reading msg
            elif "sensors" in event_data and "sensorId" in event_data:
                new_items = []
                for obj_data in event_data["sensors"]:
                    if "speed" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.SENSOR_READ_SPEED,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, obj_data))
                        # cur_board_timeline.add_items([new_timeline_item])
                    elif "pressure" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.SENSOR_READ_PRESSURE,
                                                        board_msg_original_timestamp, board_msg_id, obj_data))
                        # cur_board_timeline.add_items([new_timeline_item])
                    elif "ACCELERATOR" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.SENSOR_READ_ACCELERATOR,
                                                        board_msg_original_timestamp, board_msg_id, obj_data))
                        # cur_board_timeline.add_items([new_timeline_item])
                    elif "switchFault" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.SENSOR_READ_ELECTRIC_SWITCH,
                                                        board_msg_original_timestamp, board_msg_id, obj_data))
                    elif "detectPerson" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT,
                                                        board_msg_original_timestamp, board_msg_id, obj_data))
                    elif "cameraBlocked" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.CAMERA_BLOCKED,
                                                        board_msg_original_timestamp, board_msg_id, obj_data))
                    elif "eventType" in obj_data:
                        new_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.CAMERA_VECHILE_EVENT,
                                                        board_msg_original_timestamp, board_msg_id, obj_data))
                cur_board_timeline.add_items(new_items)
            elif "update" in event_data:
                new_update_timeline_items = [board_timeline.TimelineItem(cur_board_timeline,
                                                                         board_timeline.TimelineItemType.UPDATE_RESULT,
                                                                         board_msg_original_timestamp, board_msg_id,
                                                                         event_data)]
                cur_board_timeline.add_items(new_update_timeline_items)

    except Exception as e:
        logger.exception("Major error caused by exception:")
        print(e)
        continue
