import datetime
from logging import Logger
import os
import queue
import re
import time
from typing import List, Literal
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
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer
from tao_triton.python.device_hub.utility.session_window import *
from tao_triton.python.entrypoints import tao_client
from json import dumps
import uuid
import requests
from tao_triton.python.device_hub.event_detectors.event_detector_base import EventDetectorBase
import paho.mqtt.client as paho_mqtt_client
import json
import threading


#
# 电动车检测告警（电动车入梯)


class ElectricBicycleEnteringEventDetector(EventDetectorBase):
    SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH = "ebic_image_samples"

    # the lib triton_client used for infer to remote triton server is based on a local image file, after the infer done, the file will be cleared.
    # this is the temp folder to store that temp image file
    temp_image_files_folder_name = "temp_infer_image_files"

    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger: Logger = logging.getLogger(
            "electricBicycleEnteringEventDetectorLogger")
        self.statistics_logger = logging.getLogger("statisticsLogger")

        self.electric_bicycle_detector_name = "ElectricBicycleEnteringEventDetector"
        self.infer_server_ip_and_port = "192.168.66.149:8000"
        if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
            self.infer_server_ip_and_port = "36.153.41.18:18000"
        if util.read_config_fast_to_property(["detectors", self.electric_bicycle_detector_name], "SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH") is not None:
            ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH = util.read_config_fast_to_property(
                ["detectors", self.electric_bicycle_detector_name], "SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH")

        self.infer_from_model_worker_queue = queue.Queue()

        def infer_from_model_worker():
            while True:
                try:
                    item: board_timeline.TimelineItem = self.infer_from_model_worker_queue.get()

                    is_qua_board = item.version == "4.1qua"
                    sections = item.raw_data.split('|')
                    edge_board_confidence = sections[len(sections) - 1]
                    cropped_base64_image_file_text = sections[len(
                        sections) - 2][len("base64_image_data:"):]
                    # the last but two is the OPTIONAL, full image file with base64 encoded text,
                    # and the section is prefixed with-> full_base64_image_data:
                    # upload the full image to cloud is for debug purpose, it's controlled and cofigurable from edge board local side.
                    full_image_frame_base64_encode_text = None
                    if sections[len(sections) - 3].startswith("full_base64_image_data:"):
                        full_image_frame_base64_encode_text = sections[len(
                            sections) - 3][len("full_base64_image_data:"):]

                    infer_start_time = time.time()
                    if self.sw.state == SessionState.Uninitialized:
                        if is_qua_board:
                            next_pre_session_slient_time = util.read_config_fast_to_property(
                                ["detectors", "ElectricBicycleEnteringEventDetector"],
                                'next_pre_session_silent_time_qua')
                            post_session_silent_time = util.read_config_fast_to_property(
                                ["detectors", "ElectricBicycleEnteringEventDetector"],
                                'post_session_silent_time_qua')
                            header_buffer_end_condition = util.read_config_fast_to_property(
                                ["detectors", "ElectricBicycleEnteringEventDetector"],
                                'header_buffer_end_condition_qua')
                        else:
                            next_pre_session_slient_time = util.read_config_fast_to_property(
                                ["detectors", "ElectricBicycleEnteringEventDetector"],
                                'next_pre_session_silent_time')
                            post_session_silent_time = util.read_config_fast_to_property(
                                ["detectors", "ElectricBicycleEnteringEventDetector"],
                                'post_session_silent_time')
                            header_buffer_end_condition = util.read_config_fast_to_property(
                                ["detectors", "ElectricBicycleEnteringEventDetector"],
                                'header_buffer_end_condition')
                        if self.sw.next_pre_session_silent_time != next_pre_session_slient_time \
                                or self.sw.post_session_silent_time != post_session_silent_time \
                                or self.sw.header_buffer_end_condition != header_buffer_end_condition:
                            self.sw.next_pre_session_silent_time = next_pre_session_slient_time
                            self.sw.post_session_silent_time = post_session_silent_time
                            self.sw.header_buffer_end_condition = header_buffer_end_condition
                            self.logger.debug(
                                f"board: {self.timeline.board_id}, sw updated parameters with next_pre_session_slient_time: {next_pre_session_slient_time}, post_session_silent_time: {post_session_silent_time}, header_buffer_end_condition: {header_buffer_end_condition}")

                    is_forced_to_use_qua_model = util.read_config_fast_to_board_control_level(
                        ["detectors", 'ElectricBicycleEnteringEventDetector',
                            'ForceToUseQuaModelSwitchers'],
                        self.timeline.board_id, False)
                    qua_infer_classification_service_url = util.read_config_fast_to_property(
                        ["detectors", 'ElectricBicycleEnteringEventDetector'], "qua_infer_classification_service_url", None)
                    qua_infer_detection_service_url = util.read_config_fast_to_property(
                        ["detectors", 'ElectricBicycleEnteringEventDetector'], "qua_infer_detection_service_url", None)
                    enable_qua_detection_when_full_frame_uploaded_from_board = util.read_config_fast_to_property(
                        ["detectors", 'ElectricBicycleEnteringEventDetector'],
                        "enable_qua_detection_when_full_frame_uploaded_from_board", False)

                    if is_qua_board or is_forced_to_use_qua_model:
                        if full_image_frame_base64_encode_text and len(full_image_frame_base64_encode_text) > 1:
                            self.logger.debug(
                                f"board: {self.timeline.board_id}, see full frame image from edge board")
                            temp_full_image_file_full_name = os.path.join(self.temp_image_files_folder_name,
                                                                          str(uuid.uuid4()) + '.jpg')
                            temp_image = Image.open(io.BytesIO(base64.decodebytes(
                                full_image_frame_base64_encode_text.encode('ascii'))))
                            temp_image.save(
                                temp_full_image_file_full_name)
                            self.save_sample_image(temp_full_image_file_full_name,
                                                   item.original_timestamp,
                                                   "no_detect_yet", 1,
                                                   None,
                                                   "full_frame")
                            if enable_qua_detection_when_full_frame_uploaded_from_board:
                                detection_result: list[dict] = self.inferencer.inference_image_from_qua_detection_models(
                                    full_image_frame_base64_encode_text, service_url=qua_infer_detection_service_url)
                                if detection_result != None:
                                    # only get the ebicycle image base64 text, and then inference the classification model
                                    ebicycle_detection_result = [
                                        i for i in detection_result if i["name"] == "ebicycle" and i["conf"] >= 0.1]
                                    if len(ebicycle_detection_result) > 0:
                                        self.logger.debug(
                                            f"board: {self.timeline.board_id}, qua detection, detected ebicycle with confid: {ebicycle_detection_result[0]['conf']}, coords: {ebicycle_detection_result[0]['coords']}")
                                        cropped_base64_image_file_text = ebicycle_detection_result[
                                            0]["image_base64_encoded_text"]
                                    else:
                                        cropped_base64_image_file_text = None
                                        simplified_result = []
                                        for r in detection_result:
                                            simplified_result.append(
                                                {"name": r["name"], "conf": r["conf"], "coords": r["coords"]})
                                        self.logger.debug(
                                            f"board: {self.timeline.board_id}, qua detection, see no ebicycle result in full frame: {simplified_result}")

                        if cropped_base64_image_file_text == None or len(cropped_base64_image_file_text) < 1:
                            infered_class = 'background'
                            infer_server_current_ebic_confid = 0.0
                        else:
                            infered_class, infer_server_current_ebic_confid = self.inferencer.inference_image_from_qua_classification_models(
                                cropped_base64_image_file_text, "ebicycle", service_url=qua_infer_classification_service_url)
                            try:
                                temp_cropped_image_file_full_name = os.path.join(self.temp_image_files_folder_name,
                                                                                 str(uuid.uuid4()) + '.jpg')
                                temp_image = Image.open(io.BytesIO(base64.decodebytes(
                                    cropped_base64_image_file_text.encode('ascii'))))
                                temp_image.save(
                                    temp_cropped_image_file_full_name)
                                self.save_sample_image(temp_cropped_image_file_full_name,
                                                       item.original_timestamp,
                                                       infered_class, infer_server_current_ebic_confid,
                                                       None,
                                                       "qua_")
                            except Exception as e:
                                self.logger.exception(
                                    f"exception in save uploaded sample image: {e}")
                            finally:
                                if os.path.isfile(temp_cropped_image_file_full_name) or os.path.islink(temp_cropped_image_file_full_name):
                                    os.unlink(
                                        temp_cropped_image_file_full_name)
                    else:
                        packed_infer_result = self.inference_image_from_models(
                            item, self.current_storey)
                        if packed_infer_result == None:
                            continue
                        infered_class, infer_server_current_ebic_confid, edge_board_confidence, eb_image_base64_encode_text = packed_infer_result
                    infer_used_time_by_ms = (
                        time.time() - infer_start_time) * 1000
                    if (util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True and infer_used_time_by_ms >= 600) \
                            or (util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == False and infer_used_time_by_ms >= 250):
                        self.logger.info(
                            f"board: {self.timeline.board_id}, infer used long time: {infer_used_time_by_ms}ms")
                    self.logger.debug(
                        (f"board: {self.timeline.board_id}, adding sw item-> "
                         f"infered_class: {infered_class}, infered_confid: {infer_server_current_ebic_confid}, "
                         f"board_confid: {edge_board_confidence}, current_storey: {self.current_storey}, "
                         f"board_ori_timestamp_str: {item.original_timestamp_str}"))
                    self.sw.add({"class": infered_class,
                                 "confid": infer_server_current_ebic_confid,
                                 "confid_board": edge_board_confidence,
                                 "storey": self.current_storey,
                                 "is_qua_board": is_qua_board,
                                 "timestamp": datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat(),
                                 "cropped_base64_image_file_text": f"data:image,{cropped_base64_image_file_text}",
                                 "full_base64_image_file_text": f"data:image,{full_image_frame_base64_encode_text}"})
                except Exception as e:
                    self.logger.exception(
                        "exception in handle async task in infer_from_model_worker_queue: {}".format(e))
                finally:
                    self.infer_from_model_worker_queue.task_done()

        # Turn-on the worker thread.
        threading.Thread(target=infer_from_model_worker, daemon=True).start()

        # self.state_obj = {"last_notify_timestamp": None}
        self.alarms = []
        self.current_storey = 0

        def on_session_state_changed_to_header_buffering(item: dict):
            self.logger.info((f"board: {self.timeline.board_id}, treat as suspicious ebike entering "
                             f"""and head buffer started, will enable pre block door, current storey is: {item["storey"]}"""))
            self.alarms = []
            # send a alarm prestart msg to edge board
            self.timeline.send_mqtt_message_to_board_inbox(
                str(uuid.uuid4()), 'eb_entering_alarm_prestart', description="suspicious ebike entering")
            # send block door msg to edge for the first image, NOTE, this msg could be configed with disabled underlying in boardtimeline
            self.timeline.send_mqtt_message_to_board_inbox(
                str(uuid.uuid4()), 'enable_block_door', description="suspicious ebike entering")

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            # call back function validate the eb entering
            eb_count = 0
            is_qua_board: bool = header_buffer[0]["is_qua_board"]
            if is_qua_board:
                eb_confid = util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                                              'ebic_confid_qua')
                eb_confid_board = util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                                                    'ebic_confid_board_qua', 0.1)
                configured_eb_rate = util.read_config_fast_to_property(
                    ["detectors", "ElectricBicycleEnteringEventDetector"],
                    "eb_rate_treat_eb_entering_qua")
                header_buffer_validating_min_item_count = util.read_config_fast_to_property(
                    ["detectors", "ElectricBicycleEnteringEventDetector"],
                    "header_buffer_validating_min_item_count_qua", 3)
            else:
                eb_confid = util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                                              'ebic_confid')
                eb_confid_board = util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                                                    'ebic_confid_board', 0.1)
                configured_eb_rate = util.read_config_fast_to_property(
                    ["detectors", "ElectricBicycleEnteringEventDetector"],
                    "eb_rate_treat_eb_entering")
                header_buffer_validating_min_item_count = util.read_config_fast_to_property(
                    ["detectors", "ElectricBicycleEnteringEventDetector"],
                    "header_buffer_validating_min_item_count", 3)
            # self.logger.debug(
            #     f"board: {self.timeline.board_id}, header_buffer_validation_predict, header length: {len(header_buffer)}")
            items_log_str = "\r\n".join(
                [f"""cl: {item["class"]}, cfd: {item["confid"]}, b_cfd: {str(item["confid_board"])[:4]}""" for item in header_buffer])
            self.logger.debug(
                f"board: {self.timeline.board_id}, header_buffer_validation_predict on facts:\r\n{items_log_str}")
            for item in header_buffer:
                # self.logger.debug(
                #     f"""board: {self.timeline.board_id}, header_buffer items-> class: {item["class"]}, confid: {item["confid"]}""")
                if item["class"] == "electric_bicycle" and item["confid"] >= eb_confid and item["confid_board"] >= eb_confid_board:
                    eb_count += 1
            if is_qua_board:
                if len(header_buffer) < header_buffer_validating_min_item_count:
                    self.logger.debug(
                        f"board: {self.timeline.board_id}, header_buffer_validation_predict with False as short header length: {len(header_buffer)}")
                    return False, f"qua board, short header length: {len(header_buffer)}"
            else:
                # add a temp solution here for further boost the 1126 eb accuracy, if only got 2 and all of them are eb, then also treat as eb entering
                # if len(header_buffer) == 2:
                #     configured_eb_rate = 1
                # else:
                if len(header_buffer) < header_buffer_validating_min_item_count:
                    self.logger.debug(
                        f"board: {self.timeline.board_id}, header_buffer_validation_predict with False as short header length: {len(header_buffer)}")
                    return False, f"short header length: {len(header_buffer)}"
            eb_rate = eb_count / len(header_buffer)

            predict_result = eb_rate >= configured_eb_rate
            if predict_result:
                should_use_llm_for_post_infer = util.read_config_fast_to_board_control_level(
                    ["detectors", 'ElectricBicycleEnteringEventDetector',
                     'UseLlmForPostInferSwitchers'],
                    self.timeline.board_id, False)
                if is_qua_board or not should_use_llm_for_post_infer:
                    self.logger.info(
                        f"board: {self.timeline.board_id}, header_buffer_validation_predict with True as eb_rate: {eb_rate} >= configured_eb_rate: {configured_eb_rate}")
                else:
                    self.logger.info(
                        f"board: {self.timeline.board_id}, header_buffer_validation_predict with True as eb_rate: {eb_rate} >= configured_eb_rate: {configured_eb_rate}, further checking with llm(input image count: {len(header_buffer)})...")
                    qwen_result = self.inferencer.inference_video_by_convert_from_image_frames_from_ali_qwen_vl_model(
                        [i["cropped_base64_image_file_text"]
                         for i in header_buffer],
                        model_name="qwen-vl-plus-0809")

                    # qwen_result = self.inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
                    #     [i["cropped_base64_image_file_text"]
                    #         for i in header_buffer])
                    self.logger.debug(
                        f"board: {self.timeline.board_id}, got qwen_result: {str(qwen_result or '')}")
                    if qwen_result != None and \
                            (qwen_result["vehicle_type"] == "摩托车" or "电瓶" in qwen_result["vehicle_type"] or "电动" in qwen_result["vehicle_type"]):
                        self.logger.info(
                            f"board: {self.timeline.board_id}, qwen treat this is ebike")
                        predict_result = True
                    else:
                        self.logger.info(
                            f"board: {self.timeline.board_id}, qwen treat this is NOT ebike")
                        predict_result = False
                    return predict_result, f"is_qua: {is_qua_board}, llm: {qwen_result}, header buf-> {items_log_str}"
            else:
                self.logger.debug(
                    f"board: {self.timeline.board_id}, header_buffer_validation_predict with False as eb_rate: {eb_rate} < configured_eb_rate: {configured_eb_rate}")
            return predict_result, f"is_qua: {is_qua_board}, header buf-> {items_log_str}"

        def on_header_buffer_validated(header_buffer: list[dict], predict_data: any, is_header_buffer_valid: bool):
            is_qua_board: bool = header_buffer[0]["is_qua_board"]

            # send block door msg to edge board, ebike entring if is_header_buffer_valid is true
            # send cancel block door msg if is_header_buffer_valid is false
            if is_header_buffer_valid:
                self.logger.info(
                    f"board: {self.timeline.board_id}, will raise alarm as header buffer validated with True")
                # send a alarm start confirmation msg to edge board
                self.timeline.send_mqtt_message_to_board_inbox(
                    str(uuid.uuid4()), 'eb_entering_alarm_start', description="")
                alarms = []
                alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                    event_alarm.EventAlarmPriority.ERROR,
                    str(predict_data), "007", {}, ""))
                self.timeline.notify_event_alarm(alarms)
            else:
                self.logger.debug(
                    f"board: {self.timeline.board_id}, ebike entering condition does not met as header buffer validated with False, will cancel pre block door")
                self.timeline.send_mqtt_message_to_board_inbox(
                    str(uuid.uuid4()), 'disable_block_door',
                    description="ebike entering condition is not met")

        def on_session_end(session_window: SessionWindow[dict], session_items: list[dict]):
            # session end means the ebike is out, close the alarm and send cancel block door to local
            self.logger.info(
                "board: {}, will close alarm as session window end".format(self.timeline.board_id))
            # send a alarm end confirmation msg to edge board
            self.timeline.send_mqtt_message_to_board_inbox(
                str(uuid.uuid4()), 'eb_entering_alarm_end', description="")
            alarms = []
            alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.CLOSE,
                f"last session item-> {str(session_items[-1])}", "007"))
            self.timeline.notify_event_alarm(alarms)
            self.timeline.send_mqtt_message_to_board_inbox(
                str(uuid.uuid4()), 'disable_block_door', description="ebike session end")
            # self.sendMessageToKafka(self.cancel_block_door_message)

        def on_post_silent_time_elapsed(session_items: list[dict]):
            # reset the session window to uninitialized state and clear the list
            self.logger.debug(
                f"board: {self.timeline.board_id}, will reset the session window as post silent time elapsed")
            # 告警结束后的静默结束
            self.sw.reset()

        pre_session_slient_time = util.read_config_fast_to_property(
            ["detectors", "ElectricBicycleEnteringEventDetector"],
            'next_pre_session_silent_time')
        post_session_silent_time = util.read_config_fast_to_property(
            ["detectors", "ElectricBicycleEnteringEventDetector"],
            'post_session_silent_time')
        self.sw: SessionWindow[dict] = SessionWindow(
            lambda x: True,
            on_session_state_changed_to_header_buffering,
            BufferType.ByPeriodTime,
            util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                              "header_buffer_end_condition"),
            header_buffer_validation_predict,
            on_header_buffer_validated,
            pre_session_slient_time,
            lambda items, new_item: new_item["class"] == "electric_bicycle" and new_item[
                "confid"] > util.read_config_fast_to_property(
                ["detectors", "ElectricBicycleEnteringEventDetector"], 'body_buffer_validation_confid'
            ),
            BufferType.ByPeriodTime,
            util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                              'body_buffer_end_condition'),
            on_session_end=on_session_end,
            post_session_silent_time=post_session_silent_time,
            on_post_session_silent_time_elapsed=on_post_silent_time_elapsed
        )

        # purge previous temp files
        if os.path.exists(self.temp_image_files_folder_name):
            for filename in os.listdir(self.temp_image_files_folder_name):
                file_path = os.path.join(
                    self.temp_image_files_folder_name, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete temp_image_files_folder %s. Reason: %s' % (
                        file_path, e))
        else:
            os.makedirs(self.temp_image_files_folder_name)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.ebike_state = {"enter_time": "",
                            "exit_time": "", "latest_infer_success": ""}
        self.inferencer = Inferencer(
            self.statistics_logger, self.timeline.board_id)

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                            and "Vehicle|#|TwoWheeler" in i.raw_data and "|TwoWheeler|confirmed" not in i.raw_data) or
                           (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data))]
            return result

        return filter

    def on_mqtt_message_from_board_outbox(self, mqtt_message: paho_mqtt_client.MQTTMessage):
        """
        the callback function when a mqtt message is received from board,
        mostly used for receive the response from board for confirm the previous request
        has been received and processed in board.
        """
        str_msg = mqtt_message.payload.decode("utf-8")
        pass

    def detect(self, filtered_timeline_items):
        """
        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # temp = str(self.timeline.ebik_session.state)
        # 是否有电动车
        '''
        is_ebike_session_open = False
        if self.timeline.ebik_session and str(self.timeline.ebik_session.state) == "SessionState.OPEN":
            is_ebike_session_open = True
        '''

        eb_entering_event_alarms = []
        object_filtered_timeline_items: list[board_timeline.TimelineItem] = \
            [i for i in filtered_timeline_items if i.item_type ==
                board_timeline.TimelineItemType.OBJECT_DETECT]
        story_filtered_timeline_items = [i for i in filtered_timeline_items if
                                         i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]

        if len(story_filtered_timeline_items) > 0:
            self.current_storey = story_filtered_timeline_items[-1].raw_data["storey"]

        if abs(self.current_storey) != 1 or len(object_filtered_timeline_items) == 0:
            if abs(self.current_storey) != 1 and len(object_filtered_timeline_items) > 0:
                self.logger.debug("board: {}, edge board is uploading ebic images, but sink it due to current storey is: {}".format(
                    self.timeline.board_id, self.current_storey
                ))
                for item in object_filtered_timeline_items:
                    item.consumed = True
            return None

        try:
            for item in object_filtered_timeline_items:
                item.consumed = True
                if self.sw.state == SessionState.InPreSilentTime or self.sw.state == SessionState.InPostSilentTime:
                    self.logger.debug("board: {}, edge board is uploading ebic images, but sink it due to the sw state is: {}".format(
                        self.timeline.board_id,
                        str(self.sw.state)
                    ))
                    continue
                self.logger.debug(
                    f"board: {self.timeline.board_id}, adding board ebic image timeline item to infer_worker_queue...")
                self.infer_from_model_worker_queue.put(item)
        except Exception as e:
            self.logger.exception(
                f"board: {self.timeline.board_id}, exception in detect(...): {e}")

    # 保存图片
    def save_sample_image(self, image_file_full_name,
                          board_original_utc_timestamp: datetime,
                          infered_class, infer_server_ebic_confid,
                          full_base64_image_file_text,
                          dst_file_name_prefix=""):
        image_sample_path = os.path.join(
            ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH, self.timeline.board_id)
        current_year_month_day_str = datetime.datetime.now().strftime("%Y_%m%d")
        image_sample_path = os.path.join(
            image_sample_path, current_year_month_day_str)
        if not os.path.exists(image_sample_path):
            os.makedirs(image_sample_path)
        board_original_zone8_timestamp_str = str(board_original_utc_timestamp.astimezone(
            datetime.datetime.now().tzinfo).strftime("%Y_%m%d_%H%M_%S_%f")[:-3])
        dh_local_timestamp_str = str(
            datetime.datetime.now().strftime("%H%M_%S_%f")[:-3])
        file_name_prefix = ''
        if infered_class == 'electric_bicycle':
            pass
        else:
            file_name_prefix = infered_class + "_"
        file_name_prefix = dst_file_name_prefix + file_name_prefix
        shutil.copyfile(image_file_full_name,
                        os.path.join(
                            image_sample_path,
                            file_name_prefix + str(infer_server_ebic_confid)[
                                :4] + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + ".jpg"))
        if full_base64_image_file_text and len(full_base64_image_file_text) > 1:
            temp_full_image = Image.open(io.BytesIO(
                base64.decodebytes(full_base64_image_file_text.encode('ascii'))))
            temp_full_image.save(os.path.join(image_sample_path,
                                              str(infer_server_ebic_confid) + "___full_image__" + self.timeline.board_id + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + ".jpg"))

    def inference_image_from_models(self, object_detection_timeline_item, current_story):
        """
        run 2 classification models on the ebic image, the first model is for 4 classes, and the 2nd model is for 2 classes.
        @param object_detection_timeline_item: TimelineItem that has ebic image data which send from edge board
        @param current_story: current story of the elevator, for now only used for logging and statistics
        @return:
        """
        is_qua_board = object_detection_timeline_item.version == "4.1qua"

        infer_start_time = time.time()
        sections = object_detection_timeline_item.raw_data.split('|')

        # the last but one is the detected and cropped object image file with base64 encoded text,
        # and the section is prefixed with-> base64_image_data:
        cropped_base64_image_file_text = sections[len(
            sections) - 2][len("base64_image_data:"):]

        # the last but two is the OPTIONAL, full image file with base64 encoded text,
        # and the section is prefixed with-> full_base64_image_data:
        # upload the full image to cloud is for debug purpose, it's controlled and cofigurable from edge board local side.
        full_image_frame_base64_encode_text = sections[len(
            sections) - 3][len("full_base64_image_data:"):]

        edge_board_confidence_str: str = sections[len(sections) - 1]
        edge_board_confidence = float(str(edge_board_confidence_str)[:4])

        temp_cropped_image_file_full_name = os.path.join(self.temp_image_files_folder_name,
                                                         str(uuid.uuid4()) + '.jpg')
        temp_image = Image.open(io.BytesIO(base64.decodebytes(
            cropped_base64_image_file_text.encode('ascii'))))
        temp_image.save(temp_cropped_image_file_full_name)

        if is_qua_board and util.read_config_fast_to_property(["detectors",
                                                               self.electric_bicycle_detector_name],
                                                              'enable_infer_for_kua_board') == False:
            self.save_sample_image(temp_cropped_image_file_full_name, object_detection_timeline_item.original_timestamp,
                                   "electric_bicycle", edge_board_confidence, full_image_frame_base64_encode_text)
            self.logger.debug("qua board id:{}, infer class electric_bicycle, board confidence:{}".format(
                self.timeline.board_id,
                edge_board_confidence / 100))
            return "electric_bicycle", edge_board_confidence / 100, edge_board_confidence / 100, full_image_frame_base64_encode_text

        # only used when 2nd model declined the eb detect from 1st model, and when decling,
        # the part of 2nd model logic should have done save the sample image.
        # the next save sample image should not be called anymore.
        this_sample_image_already_saved = False
        try:
            # self.statistics_logger.debug("{} | {} | {}".format(self.timeline.board_id, "1st_model_pre_infer",""))
            raw_infer_results = tao_client.callable_main(['-m', 'elenet_four_classes_240714_tao',
                                                          '--mode', 'Classification',
                                                          '-u', self.infer_server_ip_and_port,
                                                          '--output_path', './',
                                                          temp_cropped_image_file_full_name])
            infered_class = raw_infer_results[0][0]['infer_class_name']
            infer_server_current_ebic_confid = raw_infer_results[0][0]['infer_confid']
            self.statistics_logger.debug("{} | {} | {}".format(
                self.timeline.board_id,
                "1st_model_post_infer",
                "infered_class: {}, infered_confid: {}, used_time_by_ms: {}, current_story: {}".format(
                    infered_class,
                    infer_server_current_ebic_confid,
                    str((time.time() - infer_start_time)*1000)[:6],
                    current_story)))
        except Exception as e:
            self.statistics_logger.exception("{} | {} | {}".format(
                self.timeline.board_id,
                "1st_model_post_infer",
                "exception: {}".format(e)
            ))
            self.logger.exception(
                "tao_client.callable_main(with model: elenet_four_classes_230722_tao) rasised an exception: {}".format(
                    e))
            return

        try:
            infer_start_time = time.time()
            if util.read_config_fast_to_property(
                    ["detectors",
                     ElectricBicycleEnteringEventDetector.__name__,
                     'second_infer'],
                    'enable') \
                    and infered_class == 'electric_bicycle' \
                    and infer_server_current_ebic_confid >= \
                    util.read_config_fast_to_property(["detectors",
                                                       ElectricBicycleEnteringEventDetector.__name__],
                                                      'ebic_confid') \
                    and infer_server_current_ebic_confid < \
                    util.read_config_fast_to_property(["detectors",
                                                       ElectricBicycleEnteringEventDetector.__name__,
                                                       'second_infer'],
                                                      'bypass_if_previous_model_eb_confid_greater_or_equal_than'):
                # self.statistics_logger.debug("{} | {} | {}".format(
                #     self.timeline.board_id,
                #     "2nd_model_pre_infer",
                #     ""))
                second_infer_raw_infer_results = tao_client.callable_main(['-m', 'elenet_two_classes_240705_tao',
                                                                           '--mode', 'Classification',
                                                                           '-u', self.infer_server_ip_and_port,
                                                                           '--output_path', './',
                                                                           temp_cropped_image_file_full_name])
                # classes = ['eb', 'non_eb']
                second_infer_infered_class = second_infer_raw_infer_results[0][0]['infer_class_name']
                second_infer_infered_confid = second_infer_raw_infer_results[0][0]['infer_confid']
                self.statistics_logger.debug("{} | {} | {}".format(
                    self.timeline.board_id,
                    "2nd_model_post_infer",
                    "infered_confid: {}, infered_confid: {}, used_time_by_ms: {}".format(
                        second_infer_infered_class,
                        second_infer_infered_confid,
                        str((time.time() - infer_start_time)*1000)[:6])
                ))
                if second_infer_infered_class == 'eb':
                    self.statistics_logger.debug("{} | {} | {}".format(
                        self.timeline.board_id,
                        "2nd_model_post_infer_confirm_eb",
                        "eb_confid: {}, current_story: {}".format(
                            second_infer_infered_confid, current_story)
                    ))
                    pass
                else:
                    non_eb_threshold = util.read_config_fast_to_property(["detectors",
                                                                          ElectricBicycleEnteringEventDetector.__name__,
                                                                          'second_infer'],
                                                                         'still_treat_as_eb_if_non_eb_confid_less_or_equal_than')
                    non_eb_confid = second_infer_raw_infer_results[0][0]['infer_confid']

                    # though 2nd model say it's non-eb, but for avoid too further hit down the accuracy, we still treat it as eb
                    # if the non-eb confid is less than the threshold
                    if non_eb_confid <= non_eb_threshold:
                        self.statistics_logger.debug("{} | {} | {}".format(
                            self.timeline.board_id,
                            "2nd_model_post_infer_confirm_eb",
                            "non_eb_confid: {}".format(non_eb_confid)
                        ))
                        pass
                    else:
                        self.statistics_logger.debug("{} | {} | {}".format(
                            self.timeline.board_id,
                            "2nd_model_post_infer_sink_eb",
                            "non_eb_confid: {}".format(non_eb_confid)
                        ))
                        self.logger.debug(
                            "      board: {}, sink this eb(from 4 class model) due to detect as non_eb(from 2 class model) with confid: {}"
                            .format(
                                self.timeline.board_id,
                                non_eb_confid))

                        this_sample_image_already_saved = True
                        second_model_declined__sample_image_file_name_prefix = "2nd_m_non_eb_{}___".format(
                            str(non_eb_confid)[:4])
                        self.save_sample_image(temp_cropped_image_file_full_name,
                                               object_detection_timeline_item.original_timestamp,
                                               infered_class, infer_server_current_ebic_confid,
                                               full_image_frame_base64_encode_text,
                                               second_model_declined__sample_image_file_name_prefix)
                        infered_class = 'other_unknown_stuff'
                        infer_server_current_ebic_confid = non_eb_confid
        except Exception as e:
            self.statistics_logger.exception("{} | {} | {}".format(
                self.timeline.board_id,
                "2nd_model_post_infer",
                "exception: {}".format(e)
            ))
            self.logger.exception(
                "tao_client.callable_main(with 2nd model: elenet_two_classes_240413_tao) rasised an exception: {}".format(
                    e))
            return

        t1 = time.time()

        # infer_used_time = (t1 - infer_start_time) * 1000
        # self.logger.debug(
        #     "      board: {}, time used for infer:{}ms(localConf:{}), raw infer_results/confid: {}/{}".format(
        #         self.timeline.board_id, str(infer_used_time)[:5],
        #         edge_board_confidence, infered_class, infer_server_current_ebic_confid))

        # if infered_class == 'electric_bicycle':
        try:
            if this_sample_image_already_saved == False:
                self.save_sample_image(temp_cropped_image_file_full_name,
                                       object_detection_timeline_item.original_timestamp,
                                       infered_class, infer_server_current_ebic_confid,
                                       full_image_frame_base64_encode_text)
        except:
            self.logger.exception(
                "save_sample_image(...) rasised an exception:")

        if os.path.isfile(temp_cropped_image_file_full_name) or os.path.islink(temp_cropped_image_file_full_name):
            os.unlink(temp_cropped_image_file_full_name)

        return infered_class, infer_server_current_ebic_confid, edge_board_confidence, full_image_frame_base64_encode_text
