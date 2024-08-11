import datetime
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
    # remove
    # avoid a single spike eb event mis-trigger alarm, set to 1 for disable the feature
    THROTTLE_Window_Depth = 2
    THROTTLE_Window_Time_By_Sec = 5
    # remove

    # the lib triton_client used for infer to remote triton server is based on a local image file, after the infer done, the file will be cleared.
    # this is the temp folder to store that temp image file
    temp_image_files_folder_name = "temp_infer_image_files"
    infer_server_ip_and_port = "192.168.66.161:8000"

    session: list[str] = None

    block_door_message = "Vehicle|#|TwoWheeler|TwoWheeler|confirmed"
    cancel_block_door_message = "|TwoWheeler|confirmedExit"

    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger(
            "electricBicycleEnteringEventDetectorLogger")
        self.statistics_logger = logging.getLogger("statisticsLogger")

        self.infer_from_model_worker_queue = queue.Queue()

        def worker():
            while True:
                try:
                    object_detection_timeline_item = self.infer_from_model_worker_queue.get()
                    # is_qua_board = object_detection_timeline_item.version == "4.1qua"
                    packed_infer_result = self.inference_image_from_qua_models(
                        object_detection_timeline_item)
                    if packed_infer_result == None:
                        continue
                    infered_class, infer_server_current_ebic_confid, edge_board_confidence, eb_image_base64_encode_text = packed_infer_result
                    self.sw.add({"class": infered_class, "confid": infer_server_current_ebic_confid,
                                 "storey": self.current_storey})
                except Exception as e:
                    self.logger.exception(
                        "exception in handle async task in infer_from_model_worker_queue: {}".format(e))
                finally:
                    self.infer_from_model_worker_queue.task_done()

        # Turn-on the worker thread.
        threading.Thread(target=worker, daemon=True).start()

        # self.state_obj = {"last_notify_timestamp": None}
        self.alarms = []
        self.current_storey = 0

        pre_session_slient_time = util.read_config_fast_to_property(
            ["detectors", "ElectricBicycleEnteringEventDetector"],
            'infering_stage__when_see_many_non_eb_then_enter_silent_period_duration')
        post_session_silent_time = util.read_config_fast_to_property(
            ["detectors", "ElectricBicycleEnteringEventDetector"],
            'post_session_silent_time')

        def on_header_buffer_started(item: dict):
            # send block door msg to edge for the first image
            self.alarms = []
            if (abs(item["storey"] == 1)):
                self.sendMessageToKafka(self.block_door_message)
                self.timeline.send_mqtt_message_to_board_inbox(
                    str(uuid.uuid4()), 'enable_block_door', description="enable block door as suspected ebike entering")
                self.logger.debug("board:{} head buffer started, current storey is:{}".format(self.timeline.board_id,
                                                                                              abs(item["storey"])))

        def header_buffer_validation_predict(header_buffer: list[dict]) -> bool:
            # call back function validate the eb entering
            eb_count = 0
            for item in header_buffer:
                if item["class"] == "electric_bicycle" and item["confid"] > util.read_config_fast_to_property(
                        ["detectors", "ElectricBicycleEnteringEventDetector"], 'ebic_confid'):
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            configured_eb_rate = util.read_config_fast_to_property(
                ["detectors", "ElectricBicycleEnteringEventDetector"],
                "eb_rate_treat_eb_entering"
            )
            if len(header_buffer) < 2:
                eb_rate = 0
            self.logger.debug("board:{}, header_buffer_validation_predict rate{},configured rate:{},length:{}".format(
                self.timeline.board_id, eb_rate, configured_eb_rate,
                len(header_buffer)))
            return eb_rate > configured_eb_rate

        def on_header_buffer_validated(header_buffer: list[dict], is_header_buffer_valid: bool):
            # send block door msg to edge board, ebike entring if is_header_buffer_valid is true
            # send cancel block door msg if is_header_buffer_valid is false
            if is_header_buffer_valid:
                self.alarms = []
                self.alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                          event_alarm.EventAlarmPriority.ERROR,
                                                          "detected electric-bicycle entering elevator", "007", {}, ""))
                self.timeline.send_alarms_to_web(self.alarms)

                self.logger.debug("board:{} raise alarm: head buffer valid".format(self.timeline.board_id))
            else:
                self.timeline.send_mqtt_message_to_board_inbox(
                    str(uuid.uuid4()), 'disable_block_door',
                    description="enable block door as suspected ebike entering")
                self.sendMessageToKafka(self.cancel_block_door_message)
                self.logger.debug(
                    "board:{} cancel the pre block door: head buffer invalid".format(self.timeline.board_id))

        def on_session_end(session_window: SessionWindow[dict], session_items: list[dict]):
            # self.session = session_items
            # session end means the ebike is out, close the alarm and send cancel block door to local
            self.alarms = []
            self.alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                      event_alarm.EventAlarmPriority.CLOSE,
                                                      "close the EBike alarm", "007"))
            self.timeline.send_alarms_to_web(self.alarms)
            self.timeline.send_mqtt_message_to_board_inbox(
                str(uuid.uuid4()), 'disable_block_door', description="enable block door as suspected ebike entering")
            self.sendMessageToKafka(self.cancel_block_door_message)
            self.logger.debug("board:{} close alarm: session window end".format(self.timeline.board_id))

        def on_post_silent_time_elapsed(session_items: list[dict]):
            # reset the session window to uninitialized state and clear the list
            # 告警结束后的静默结束
            self.sw.reset()
            self.logger.debug("board:{} reset session window: post silent time elapsed".format(self.timeline.board_id))

        self.sw: SessionWindow[dict] = SessionWindow(
            lambda x: True,
            BufferType.ByPeriodTime,
            util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                              "how_long_to_treat_header_buffer_end"),
            header_buffer_validation_predict,
            on_header_buffer_validated,
            pre_session_slient_time,
            lambda items, new_item: new_item["class"] == "electric_bicycle" and new_item[
                "confid"] > util.read_config_fast_to_property(
                ["detectors", "ElectricBicycleEnteringEventDetector"], 'keep_ebic_confid'
            ),
            BufferType.ByPeriodTime,
            util.read_config_fast_to_property(["detectors", "ElectricBicycleEnteringEventDetector"],
                                              'time_to_keep_successful_infer_result'),
            on_session_end=on_session_end,
            post_session_silent_time=post_session_silent_time,
            on_post_session_silent_time_elapsed=on_post_silent_time_elapsed,
            on_header_buffer_started=on_header_buffer_started
        )

        if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
            self.infer_server_ip_and_port = "36.153.41.18:18000"

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
        object_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        story_filtered_timeline_items = [i for i in filtered_timeline_items if
                                         i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]

        if len(story_filtered_timeline_items) > 0:
            self.current_storey = story_filtered_timeline_items[-1].raw_data["storey"]

        if abs(self.current_storey) != 1 or len(object_filtered_timeline_items) == 0:
            if abs(self.current_storey) != 1 and len(object_filtered_timeline_items) > 0:
                self.logger.debug("board:{} sink the image due to current storey is :{}".format(
                    self.timeline.board_id, self.current_storey
                ))
                for item in object_filtered_timeline_items:
                    item.consumed = True
            return None

        try:
            for item in object_filtered_timeline_items:
                item.consumed = True
                if self.sw.state == SessionState.InPreSilentTime or self.sw.state == SessionState.InPostSilentTime:
                    self.logger.debug("board:{} sink the image due to the session window state is:{}".format(
                        self.timeline.board_id,
                        str(self.sw.state)
                    ))
                    continue
                is_qua_board = item.version == "4.1qua"
                if is_qua_board:
                    self.infer_from_model_worker_queue.put(item)
                else:
                    packed_infer_result = self.inference_image_from_models(item, self.current_storey)
                    if packed_infer_result == None:
                        continue
                    infered_class, infer_server_current_ebic_confid, edge_board_confidence, eb_image_base64_encode_text = packed_infer_result
                    self.sw.add({"class": infered_class, "confid": infer_server_current_ebic_confid,
                                 "storey": self.current_storey})
        except Exception as e:
            self.logger.exception("{} | {} | {}".format(
                self.timeline.board_id, "feed", "exception: {}".format(e)))

    def save_sample_image(self, image_file_full_name, original_utc_timestamp: datetime, infered_class,
                          infer_server_ebic_confid, full_base64_image_file_text, dst_file_name_prefix=""):
        image_sample_path = os.path.join(
            ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH, self.timeline.board_id)
        current_year_month_day_str = datetime.datetime.now().strftime("%Y_%m%d")
        image_sample_path = os.path.join(image_sample_path, current_year_month_day_str)
        if not os.path.exists(image_sample_path):
            os.makedirs(image_sample_path)
        board_original_zone8_timestamp_str = str(original_utc_timestamp.astimezone(
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
            temp_full_image.save(os.path.join(
                image_sample_path,
                str(infer_server_ebic_confid) + "___full_image__" + self.timeline.board_id + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + ".jpg"))

    def sendMessageToKafka(self, message):
        if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
            return
        try:
            config_item = [
                i for i in self.timeline.configures if i["code"] == "kqzt"]
            config_kqzt = 1 if len(config_item) == 0 else int(
                config_item[0]["value"])
            config_close_zt = [
                i for i in self.timeline.configures if i["code"] == "gbzt"]
            gbzt = False if len(config_close_zt) == 0 else (
                    self.timeline.board_id in config_close_zt[0]["value"])
            if (config_kqzt == 0 or gbzt) and "|TwoWheeler|confirmed" in message:
                self.logger.info("board:{} is in configured to disable block door".format(
                    self.timeline.board_id))
                return
            # self.logger.info("------------------------board:{} is not in the target list".format(self.timeline.board_id))
            # producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
            #                         value_serializer=lambda x: dumps(x).encode('utf-8'))
            self.logger.info("board:{} send message:{} to kafka".format(
                self.timeline.board_id, message))
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
                "infered_class: {}, infered_confid: {}, used_time: {}, current_story: {}".format(
                    infered_class,
                    infer_server_current_ebic_confid,
                    str(time.time() - infer_start_time)[:5],
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
                    "infered_confid: {}, infered_confid: {}".format(
                        second_infer_infered_class, second_infer_infered_confid)
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

        infer_used_time = (t1 - infer_start_time) * 1000
        self.logger.debug(
            "      board: {}, time used for infer:{}ms(localConf:{}), raw infer_results/confid: {}/{}".format(
                self.timeline.board_id, str(infer_used_time)[:5],
                edge_board_confidence, infered_class, infer_server_current_ebic_confid))

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

    def inference_image_from_qua_models(self, object_detection_timeline_item):
        sections = object_detection_timeline_item.raw_data.split('|')
        # the last but one is the detected and cropped object image file with base64 encoded text,
        # and the section is prefixed with-> base64_image_data:
        cropped_base64_image_file_text = sections[len(
            sections) - 2][len("base64_image_data:"):]
        # the last but two is the OPTIONAL, full image file with base64 encoded text,
        # and the section is prefixed with-> full_base64_image_data:
        # upload the full image to cloud is for debug purpose, it's controlled and cofigurable from edge board local side.
        full_image_frame_base64_encode_text = sections[len(sections) - 3][len("full_base64_image_data:"):]
        edge_board_confidence_str: str = sections[len(sections) - 1]
        edge_board_confidence = float(str(edge_board_confidence_str)[:4])
        temp_cropped_image_file_full_name = os.path.join(self.temp_image_files_folder_name,
                                                         str(uuid.uuid4()) + '.jpg')
        temp_image = Image.open(io.BytesIO(base64.decodebytes(
            cropped_base64_image_file_text.encode('ascii'))))
        temp_image.save(temp_cropped_image_file_full_name)
        infer_start_time = time.time()
        infer_server_url = "http://36.139.163.39:18090/detect_images"
        data = {'input_image': cropped_base64_image_file_text,
                'confidence_hold': 0.35}
        http_response = requests.post(infer_server_url, json=data)

        t1 = time.time()
        infer_used_time = (t1 - infer_start_time) * 1000

        infer_results = json.loads(http_response.text)

        # ['bicycle', 'ebicycle']
        infered_class_raw = infer_results['name']
        if infered_class_raw == 'ebicycle':
            infered_class = 'electric_bicycle'
        else:
            infered_class = 'bicycle'
        infered_server_confid = infer_results['confidence']
        self.logger.debug(
            "      board: {}, time used for qua infer: {}ms(localConf: {}), raw infer_results/confid: {}/{}".format(
                self.timeline.board_id, str(infer_used_time)[:5],
                edge_board_confidence, infered_class, infered_server_confid))
        self.statistics_logger.debug("{} | {} | {}".format(
            self.timeline.board_id,
            "1st_qua_model_post_infer",
            "infered_class: {}, infered_confid: {}, used_time: {}, thread_active_count: {}".format(
                infered_class,
                infered_server_confid,
                str(infer_used_time)[:5], threading.active_count())))
        self.save_sample_image(temp_cropped_image_file_full_name,
                               object_detection_timeline_item.original_timestamp,
                               infered_class, infered_server_confid,
                               full_image_frame_base64_encode_text, "qua_")
        if os.path.isfile(temp_cropped_image_file_full_name) or os.path.islink(temp_cropped_image_file_full_name):
            os.unlink(temp_cropped_image_file_full_name)
        return infered_class, infered_server_confid, edge_board_confidence, full_image_frame_base64_encode_text
