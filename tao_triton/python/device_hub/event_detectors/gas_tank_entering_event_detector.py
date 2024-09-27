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
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer
from tao_triton.python.device_hub.utility.session_window import *
from tao_triton.python.entrypoints import tao_client
from json import dumps
import uuid
import requests
from tao_triton.python.device_hub.event_detectors.event_detector_base import EventDetectorBase


#
# 煤气罐检测告警（煤气罐入梯)


class GasTankEnteringEventDetector(EventDetectorBase):
    SAVE_IMAGE_SAMPLE_ROOT_FOLDER_PATH = "gastank_image_samples"
    Enable_SAVE_IMAGE_SAMPLE = True

    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger("gasTankEnteringEventDetectorLogger")
        self.statistics_logger = logging.getLogger("statisticsLogger")
        self.need_close_alarm = False
        self.silent_period_duration = util.read_config_fast_to_property(
            ["detectors", "GasTankEnteringEventDetector"], 'silent_period_duration')
        self.body_buffer_end_period_duration = util.read_config_fast_to_property(
            ["detectors", "GasTankEnteringEventDetector"], 'how_long_to_treat_tank_exist_when_no_predict_item_received')

        def header_buffer_starter_validation(item: dict) -> bool:
            gas_tank_confid = util.read_config_fast_to_property(
                ["detectors", "GasTankEnteringEventDetector"], 'gas_tank_confid')
            return item["class"] == "gastank" and item["confid"] > gas_tank_confid

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            gas_tank_count = 0
            configured_gas_tank_rate = util.read_config_fast_to_property(
                ["detectors", "GasTankEnteringEventDetector"],
                "gas_tank_rate_treat_tank_entering")
            gas_tank_confid = util.read_config_fast_to_property(
                ["detectors", "GasTankEnteringEventDetector"], 'gas_tank_confid')
            for item in header_buffer:
                if item["class"] == "gastank" and item["confid"] > gas_tank_confid:
                    gas_tank_count += 1
            return gas_tank_count / len(header_buffer) > configured_gas_tank_rate, "gas_tank"

        def on_header_buffer_validated(buffer: list[dict], predict_data: any, is_header_buffer_valid: bool) -> None:
            self.logger.debug("board:{},header buffer validated result:{}".format(self.timeline.board_id,
                                                                                  str(is_header_buffer_valid)))
            # 进入body_buffering状态
            if is_header_buffer_valid:
                infered_class = buffer[-1]["class"]
                infered_confid = buffer[-1]["confid"]
                event_alarms = []
                event_alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                    event_alarm.EventAlarmPriority.ERROR,
                    f"detected gas tank entering elevator with infered_class: {infered_class}, infered_confid: {infered_confid}"))
                self.timeline.notify_event_alarm(event_alarms)
                self.logger.debug(
                    "board:{}, raise gas tank alarm".format(self.timeline.board_id))

        def body_buffer_validation(items: list[dict], item: dict) -> bool:
            gas_tank_confid = util.read_config_fast_to_property(
                ["detectors", "GasTankEnteringEventDetector"], 'gas_tank_confid')
            return item["class"] == "gastank" and item["confid"] > gas_tank_confid

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]) -> None:
            # self.assertEqual(sw.state, SessionState.SessionEnd)
            # # session 结束，结束告警
            self.logger.debug(
                "board:{}, session end, close the alarm".format(self.timeline.board_id))
            event_alarms = []
            event_alarms.append(
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                    event_alarm.EventAlarmPriority.CLOSE,
                    "Close gas tank alarm", "0021"))
            self.timeline.notify_event_alarm(event_alarms)

        def on_post_session_silent_time_elapsed(items: list[dict]):
            self.logger.debug(
                "board:{}, post silent session end".format(self.timeline.board_id))
            self.sw.reset()

        self.sw: SessionWindow[dict] = SessionWindow(
            header_buffer_starter_validation, None, BufferType.ByPeriodTime, 4,
            header_buffer_validation_predict, on_header_buffer_validated, self.silent_period_duration,
            body_buffer_validation, BufferType.ByPeriodTime, self.body_buffer_end_period_duration,
            on_session_end=on_session_end,
            post_session_silent_time=self.silent_period_duration,
            on_post_session_silent_time_elapsed=on_post_session_silent_time_elapsed)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.state_obj = {"last_infer_timestamp": None,
                          "last_notify_timestamp": None}
        self.timeline = timeline
        self.inferencer = Inferencer(
            self.statistics_logger, self.timeline.board_id)

    '''
    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            result = [i for i in timeline_items if
                      not i.consumed and
                      (i.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP or (
                              i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT))]
            # and "Vehicle|#|gastank" in i.raw_data))]
            return result

        return filter
    '''

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        is_enabled = util.read_config_fast_to_board_control_level(
            ["detectors", 'GasTankEnteringEventDetector', 'FeatureSwitchers'], self.timeline.board_id)

        gas_tank_items = [i for i in filtered_timeline_items if (not i.consumed and
                                                                 i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                                                                 "Vehicle|#|gastank" in i.raw_data)]

        if len(gas_tank_items) > 0:
            self.logger.debug(
                f"board: {self.timeline.board_id}, see gas_tank_item from edge board")
            item = gas_tank_items[-1]
            for item in gas_tank_items:
                item.consumed = True

            sections = item.raw_data.split('|')
            edge_board_confidence = sections[len(sections) - 1]
            cropped_base64_image_file_text = sections[len(
                sections) - 2][len("base64_image_data:"):]
            if self.Enable_SAVE_IMAGE_SAMPLE:
                image_sample_path = os.path.join(
                    GasTankEnteringEventDetector.SAVE_IMAGE_SAMPLE_ROOT_FOLDER_PATH, self.timeline.board_id)
                if not os.path.exists(image_sample_path):
                    os.makedirs(image_sample_path)
                file_name_timestamp_str = str(
                    datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")[:-3])
                # shutil.copyfile(os.path.join("temp_infer_image_files", "0.jpg"),
                #                 os.path.join(
                #                     ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_FOLDER_PATH,
                #                     str(infer_server_ebic_confid) + "___" + self.timeline.board_id + "___" + file_name_timestamp_str + ".jpg"))

                if cropped_base64_image_file_text and len(cropped_base64_image_file_text) > 1:
                    temp_cropped_image = Image.open(io.BytesIO(
                        base64.decodebytes(cropped_base64_image_file_text.encode('ascii'))))
                    temp_cropped_image.save(os.path.join(
                        image_sample_path,
                        "crop_image___" + file_name_timestamp_str + "___" + self.timeline.board_id + ".jpg"))

            if is_enabled:
                def infer_and_post_process(infered_result):
                    infered_class, infered_confid = infered_result
                    self.logger.debug(
                        f"board: {self.timeline.board_id}, infered_class: {infered_class}, infered_confid: {infered_confid}")

                    self.sw.add({"class": infered_class,
                                "confid": infered_confid})

                enable_async_infer_and_post_process = util.read_config_fast_to_property(
                    ["detectors", 'GasTankEnteringEventDetector'], 'enable_async_infer_and_post_process')
                if enable_async_infer_and_post_process:
                    self.inferencer.start_inference_image_from_qua_models(
                        cropped_base64_image_file_text, "gastank", infer_and_post_process)
                else:
                    infered_class, infered_confid = self.inferencer.inference_image_from_qua_models(
                        cropped_base64_image_file_text, "gastank")
                    infer_and_post_process((infered_class, infered_confid))
        return None

    def sendMessageToKafka(self, message):
        try:
            config_item = [
                i for i in self.timeline.configures if i["code"] == "mqgbzt"]
            config_kqzt = False if len(config_item) == 0 else (
                self.timeline.board_id not in config_item[0]["value"])
            if self.timeline.board_id in self.timeline.target_borads or config_kqzt == False:
                return
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
