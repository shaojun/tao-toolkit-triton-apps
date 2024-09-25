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
from tao_triton.python.entrypoints import tao_client
from json import dumps
import uuid
import requests
from tao_triton.python.device_hub.event_detectors.event_detector_base import EventDetectorBase
#
# 电池检测告警（电池入梯)


class BatteryEnteringEventDetector(EventDetectorBase):
    SAVE_IMAGE_SAMPLE_ROOT_FOLDER_PATH = "battery_image_samples"
    Enable_SAVE_IMAGE_SAMPLE = True

    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger("batteryEnteringEventDetectorLogger")
        self.statistics_logger = logging.getLogger("statisticsLogger")
        self.need_close_alarm = False

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
        self.inferencer = Inferencer(self.logger, self.timeline.board_id)
        

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        is_enabled = util.read_config_fast_to_board_control_level(
            ["detectors", 'BatteryEnteringEventDetector', 'FeatureSwitchers'], self.timeline.board_id)

        event_alarms = []

        # 告警时间超5秒关闭告警
        if self.need_close_alarm and self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj[
            "last_notify_timestamp"] != None and \
                (datetime.datetime.now() - self.state_obj["last_notify_timestamp"]).total_seconds() > 30:
            self.logger.debug(
                "board:{}, close battery entering alarm".format(self.timeline.board_id))

            self.need_close_alarm = False
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            self.sendMessageToKafka("|TwoWheeler|confirmedExit")
            event_alarms.append(
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                    event_alarm.EventAlarmPriority.CLOSE,
                    "Close battery alarm", "0024"))
            return event_alarms

        battery_items = [i for i in filtered_timeline_items if (not i.consumed and
                                                                i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                                                                "Vehicle|#|battery" in i.raw_data)]

        # 记录最近一次获取到battery的时间暂时用不上
        if len(battery_items) > 0:
            self.state_obj["last_infer_timestamp"] = datetime.datetime.now()

        if self.need_close_alarm:
            for item in battery_items:
                item.consumed = True
            return None

        silent_period_duration = util.read_config_fast_to_property(
            ["detectors", "BatteryEnteringEventDetector"],
            'silent_period_duration')
        # 上一次告警结束不足60s内不再告警
        if "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"] != None and \
                (datetime.datetime.now() - self.state_obj[
                    "last_notify_timestamp"]).total_seconds() < silent_period_duration:
            return None

        if len(battery_items) > 0:
            self.logger.debug(
                f"board: {self.timeline.board_id}, see battery_item from edge board")
            item = battery_items[-1]
            for item in battery_items:
                item.consumed = True

            sections = item.raw_data.split('|')
            edge_board_confidence = sections[len(sections) - 1]
            cropped_base64_image_file_text = sections[len(
                sections) - 2][len("base64_image_data:"):]
            if self.Enable_SAVE_IMAGE_SAMPLE:
                image_sample_path = os.path.join(
                    BatteryEnteringEventDetector.SAVE_IMAGE_SAMPLE_ROOT_FOLDER_PATH, self.timeline.board_id)
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
                    if infered_class == "battery" and infered_confid >= 0.9:
                        self.statistics_logger.debug("{} | {} | {}".format(
                            self.timeline.board_id,
                            "BatteryEnteringEventDetector_confirm_battery",
                            "data: {}".format("")
                        ))
                        self.logger.debug(
                            f"board: {self.timeline.board_id}, raise battery entering alarm, infered_class: {infered_class},\
                            infered_server_confid: {infered_confid}, edge_board_confidence{edge_board_confidence}")
                        self.state_obj["last_notify_timestamp"] = datetime.datetime.now(
                        )
                        self.need_close_alarm = True
                        # self.sendMessageToKafka(
                        #     "Vehicle|#|TwoWheeler" + "|TwoWheeler|confirmed")
                        # self.sendMessageToKafka("test info")
                        event_alarms.append(
                            event_alarm.EventAlarm(self, item.original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                                   f"detected battery entering elevator with board confid: {edge_board_confidence}, infered_class: {infered_class}, infered_confid: {infered_confid}"))
                        self.timeline.notify_event_alarm(event_alarms)
                enable_async_infer_and_post_process = util.read_config_fast_to_property(
                    ["detectors", 'BatteryEnteringEventDetector', 'FeatureSwitchers'], 'enable_async_infer_and_post_process')

                if enable_async_infer_and_post_process:
                    self.inferencer.start_inference_image_from_qua_models(
                        cropped_base64_image_file_text, "battery", infer_and_post_process)
                else:
                    infered_class, infered_confid = self.inferencer.inference_image_from_qua_models(
                        cropped_base64_image_file_text, "battery")
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
