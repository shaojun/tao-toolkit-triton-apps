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
import json
import uuid

# infer_server_url = None
# infer_server_protocol = None
# infer_model_name = None
# infer_model_version = None
# infer_server_comm_output_verbose = None
from python.device_hub import board_timeline
from python.device_hub.board_timeline import BoardTimeline
from python.device_hub.timeline_event_alarm import EventAlarmDummyNotifier
from python.device_hub.timeline_event_detectors import *

if __name__ == '__main__':
    with open('log_config.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)
    logger.info('%s is starting...', 'device_hub')
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
                        default='dev-iot.ipos.biz:9092',
                        help='kafka server URL. Default is xxx:9092.')
    FLAGS = parser.parse_args()
try:
    consumer = KafkaConsumer(
        bootstrap_servers=FLAGS.kafka_server_url,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=str(uuid.uuid1()),
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    consumer.subscribe(pattern=".*")
    # do a dummy poll to retrieve some message
    consumer.poll()

    # go to end of the stream
    consumer.seek_to_end()

    boards_timelines = []

    for event in consumer:
        event_data = event.value
        if "sensorId" not in event_data or "@timestamp" not in event_data:
            continue
        board_msg_id = event_data["id"]
        board_msg_original_timestamp = event_data["@timestamp"]
        board_id = event_data["sensorId"]
        cur_board_timeline = [t for t in boards_timelines if
                              t.board_id == board_id]
        if not cur_board_timeline:
            cur_board_timeline = \
                BoardTimeline(board_id, [],
                              [DoorStateChangedEventDetector(logging),
                               ElectricBicycleEnteringEventDetector(logging),
                               BlockingDoorEventDetector(logging),
                               PeopleStuckEventDetector(logging)],
                              [EventAlarmDummyNotifier(logging)])
            boards_timelines.append(cur_board_timeline)
        else:
            cur_board_timeline = cur_board_timeline[0]
        # indicates it's the object detection msg
        if "objects" in event_data:
            for obj_data in event_data["objects"]:
                new_timeline_item = \
                    TimelineItem(TimelineItemType.OBJECT_DETECT,
                                 board_msg_original_timestamp,
                                 board_msg_id,
                                 obj_data)
                cur_board_timeline.add_item(new_timeline_item)

        # indicates it's the sensor data reading msg
        elif "sensors" in event_data and "sensorId" in event_data:
            for obj_data in event_data["sensors"]:
                if "speed" in obj_data:
                    new_timeline_item \
                        = TimelineItem(TimelineItemType.SENSOR_READ_SPEED,
                                       board_msg_original_timestamp,
                                       board_msg_id,
                                       obj_data)
                    cur_board_timeline.add_item(new_timeline_item)
                elif "pressure" in obj_data:
                    new_timeline_item \
                        = TimelineItem(TimelineItemType.SENSOR_READ_PRESSURE,
                                       board_msg_original_timestamp,
                                       board_msg_id,
                                       obj_data)
                    cur_board_timeline.add_item(new_timeline_item)
                elif "ACCELERATOR" in obj_data:
                    new_timeline_item \
                        = TimelineItem(TimelineItemType.SENSOR_READ_ACCELERATOR,
                                       board_msg_original_timestamp,
                                       board_msg_id,
                                       obj_data)
                    cur_board_timeline.add_item(new_timeline_item)

except:
    logger.exception("Major error caused by unhandled exception:")
