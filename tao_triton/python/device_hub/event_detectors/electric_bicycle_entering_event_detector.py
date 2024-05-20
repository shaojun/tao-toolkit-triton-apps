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
#
# 电动车检测告警（电动车入梯)
class ElectricBicycleEnteringEventDetector(EventDetectorBase):
    SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH = "ebic_image_samples"

    # avoid a single spike eb event mis-trigger alarm, set to 1 for disable the feature
    THROTTLE_Window_Depth = 2
    THROTTLE_Window_Time_By_Sec = 5

    # the lib triton_client used for infer to remote triton server is based on a local image file, after the infer done, the file will be cleared.
    # this is the temp folder to store that temp image file
    temp_image_files_folder_name = "temp_infer_image_files"

    infer_server_ip_and_port = "127.0.0.1:8000"
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger(
            "electricBicycleEnteringEventDetectorLogger")
        self.statistics_logger = logging.getLogger(
            "statisticsLogger")
        # self.logger.debug('{} is initing...'.format('ElectricBicycleEnteringEventDetector'))
        self.infer_confirmed_eb_history_list = []
        self.local_ebike_infer_result_list = []
        self.alarm_raised = False
        self.close_alarm = False
        if util.read_fast_from_app_config_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
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
            # and abs((datetime.datetime.now(
            #           datetime.timezone.utc) - i.original_timestamp).total_seconds()) < 100))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        eb_entering_event_alarms = []
        object_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        story_filtered_timeline_items = [i for i in filtered_timeline_items if
                                         i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
        current_story = 0
        if len(story_filtered_timeline_items) > 0:
            current_story = story_filtered_timeline_items[-1].raw_data["storey"]
        # self.logger.debug(
        #     "ElectricBicycleEnteringEventDetector object len:{},story_length:{},board:{},current_story:{}".format(
        #         len(object_filtered_timeline_items),
        #         len(story_filtered_timeline_items),
        #         self.timeline.board_id, current_story))
        if len(object_filtered_timeline_items) == 0:
            ebike_confid_threshold = util.read_fast_from_app_config_to_property(
                ["detectors", ElectricBicycleEnteringEventDetector.__name__],
                'ebic_confid')
            self.sendEBikeConfirmToLocal(0, ebike_confid_threshold)
            self.raiseTheAlarm(0, ebike_confid_threshold)

        for item in object_filtered_timeline_items:
            item.consumed = True
            if self.state_obj and "last_infer_ebic_timestamp" in self.state_obj:
                # we don't want to report too freq
                last_infer_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_infer_ebic_timestamp"]).total_seconds()
                # self.logger.debug("last_infer_time_diff:{}".format(last_infer_time_diff))
                # if last_infer_time_diff < 2:
                self.state_obj = {}
                self.logger.debug(
                    "skip one ebike object due to last failure result")
                if last_infer_time_diff < 2:
                    self.logger.debug(
                        "skip one ebike object due to last failure result:{}".format(last_infer_time_diff))
                    continue
            if self.state_obj and "last_report_timestamp" in self.state_obj:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_report_timestamp"]).total_seconds()
                self.logger.debug(
                    "last_report_time_diff:{}".format(last_report_time_diff))
                if last_report_time_diff <= 45:
                    continue
            sections = item.raw_data.split('|')
            # self.logger.debug(
            #     "board: {}, E-bic is detected and re-infer it from triton...".format(item.timeline.board_id))

            # the last but one is the detected and cropped object image file with base64 encoded text,
            # and the section is prefixed with-> base64_image_data:
            cropped_base64_image_file_text = sections[len(
                sections) - 2][len("base64_image_data:"):]

            # the last but two is the OPTIONAL, full image file with base64 encoded text,
            # and the section is prefixed with-> full_base64_image_data:
            # upload the full image to cloud is for debug purpose, it's controlled and cofigurable from edge board local side.
            full_base64_image_file_text = sections[len(
                sections) - 3][len("full_base64_image_data:"):]

            edge_board_confidence = sections[len(sections) - 1]
            # infer_results = base64_tao_client.infer(FLAGS.verbose, FLAGS.async_set, FLAGS.streaming,
            #                                         FLAGS.model_name, FLAGS.model_version,
            #                                         FLAGS.batch_size, FLAGS.class_list,
            #                                         False, FLAGS.url, FLAGS.protocol, FLAGS.mode,
            #                                         FLAGS.output_path,
            #                                         [cropped_base64_image_file_text])
            # 推理服务器36.153.41.18:18000
            temp_cropped_image_file_full_name = os.path.join(self.temp_image_files_folder_name,
                                                             str(uuid.uuid4()) + '.jpg')
            temp_image = Image.open(io.BytesIO(base64.decodebytes(
                cropped_base64_image_file_text.encode('ascii'))))
            temp_image.save(temp_cropped_image_file_full_name)
            infer_start_time = time.time()

            # only used when 2nd model declined the eb detect from 1st model, and when decling, we should have done save the sample image
            # the next save sample image should not be called anymore.
            this_sample_image_already_saved = False
            try:
                # self.statistics_logger.debug("{} | {} | {}".format(self.timeline.board_id, "1st_model_pre_infer",""))
                raw_infer_results = tao_client.callable_main(['-m', 'elenet_four_classes_230722_tao',
                                                              '--mode', 'Classification',
                                                              '-u', self.infer_server_ip_and_port,
                                                              '--output_path', './',
                                                              temp_cropped_image_file_full_name])
                infered_class = raw_infer_results[0][0]['infer_class_name']
                infer_server_current_ebic_confid = raw_infer_results[0][0]['infer_confid']
                self.statistics_logger.debug("{} | {} | {}".format(
                    self.timeline.board_id, 
                    "1st_model_post_infer",
                    "infered_class: {}, infered_confid: {}, used_time: {}".format(
                        infered_class,
                        infer_server_current_ebic_confid,
                        str(time.time() - infer_start_time)[:5])))
            except Exception as e:
                self.statistics_logger.exception("{} | {} | {}".format(
                    self.timeline.board_id, 
                    "1st_model_post_infer",
                    "exception: {}".format(e)
                    ))
                self.logger.exception(  
                    "tao_client.callable_main(with model: elenet_four_classes_230722_tao) rasised an exception: {}".format(e))
                return
            
            try:
                if util.read_fast_from_app_config_to_property(["detectors", 
                                                               ElectricBicycleEnteringEventDetector.__name__,
                                                               'second_infer'], 
                                                               'enable') \
                    and infered_class == 'electric_bicycle' \
                    and infer_server_current_ebic_confid >= util.read_fast_from_app_config_to_property(["detectors", 
                                                                 ElectricBicycleEnteringEventDetector.__name__],
                                                                'ebic_confid')\
                    and infer_server_current_ebic_confid < util.read_fast_from_app_config_to_property(["detectors", 
                                                               ElectricBicycleEnteringEventDetector.__name__,
                                                               'second_infer'], 
                                                               'bypass_if_previous_model_eb_confid_greater_or_equal_than'):
                    # self.statistics_logger.debug("{} | {} | {}".format(
                    #     self.timeline.board_id, 
                    #     "2nd_model_pre_infer",
                    #     ""))
                    second_infer_raw_infer_results = tao_client.callable_main(['-m', 'elenet_two_classes_240413_tao',
                                                                '--mode', 'Classification',
                                                                '-u', self.infer_server_ip_and_port,
                                                                '--output_path', './',
                                                                temp_cropped_image_file_full_name])
                    #classes = ['eb', 'non_eb']
                    second_infer_infered_class = second_infer_raw_infer_results[0][0]['infer_class_name']
                    second_infer_infered_confid = second_infer_raw_infer_results[0][0]['infer_confid']
                    self.statistics_logger.debug("{} | {} | {}".format(
                        self.timeline.board_id,
                        "2nd_model_post_infer",
                        "infered_confid: {}, infered_confid: {}".format(second_infer_infered_class,second_infer_infered_confid) 
                    ))
                    if second_infer_infered_class == 'eb':
                        self.statistics_logger.debug("{} | {} | {}".format(
                            self.timeline.board_id,
                            "2nd_model_post_infer_confirm_eb",
                            "eb_confid: {}".format(second_infer_infered_confid)
                            ))
                        pass
                    else:
                        non_eb_threshold = util.read_fast_from_app_config_to_property(["detectors", 
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
                            second_model_declined__sample_image_file_name_prefix = "2nd_m_non_eb_{}___".format(str(non_eb_confid)[:4])
                            self.save_sample_image(temp_cropped_image_file_full_name, item.original_timestamp,
                                       infered_class, infer_server_current_ebic_confid, full_base64_image_file_text, second_model_declined__sample_image_file_name_prefix)
                            # gives a fake class of eb and confid to make sure the following logic can be executed
                            # following logic will treat this as eb with low confid and will not trigger alarm, but other logic will still be executed
                            infered_class = 'electric_bicycle'
                            infer_server_current_ebic_confid = 0.119
            except Exception as e:
                self.statistics_logger.exception("{} | {} | {}".format(
                    self.timeline.board_id, 
                    "2nd_model_post_infer",
                    "exception: {}".format(e)
                    ))
                self.logger.exception(
                    "tao_client.callable_main(with 2nd model: elenet_two_classes_240413_tao) rasised an exception: {}".format(e))
                return

            t1 = time.time()

            infer_used_time = (t1 - infer_start_time) * 1000
            self.logger.debug(
                "      board: {}, time used for infer:{}ms(localConf:{}), raw infer_results/confid: {}/{}".format(
                    self.timeline.board_id, str(infer_used_time)[:5],
                    str(edge_board_confidence)[:4], infered_class, infer_server_current_ebic_confid))

            # if infered_class == 'electric_bicycle':
            try:
                if this_sample_image_already_saved == False:
                    self.save_sample_image(temp_cropped_image_file_full_name, item.original_timestamp,
                                        infered_class, infer_server_current_ebic_confid, full_base64_image_file_text)
            except:
                self.logger.exception(
                    "save_sample_image(...) rasised an exception:")
            if os.path.isfile(temp_cropped_image_file_full_name) or os.path.islink(temp_cropped_image_file_full_name):
                os.unlink(temp_cropped_image_file_full_name)
            if infered_class != 'electric_bicycle':
                self.logger.debug(
                    "      board: {}, rewrite this eb detect due to infer server treat as non-eb class at all: {}".format(
                        self.timeline.board_id,
                        infered_class))
                infered_class = 'electric_bicycle'
                infer_server_current_ebic_confid = 0.1212
            if infered_class != 'electric_bicycle':
                self.logger.debug(
                    "      board: {}, sink this eb detect due to infer server treat as non-eb class at all: {}".format(
                        self.timeline.board_id,
                        infered_class))
                continue

            temp = self.__process_infer_result__(
                item.original_timestamp, edge_board_confidence, infer_server_current_ebic_confid, current_story,
                cropped_base64_image_file_text)
            eb_entering_event_alarms.extend(temp)
            # if eb_entering_event_alarms and len(eb_entering_event_alarms) > 0:
            #    self.state_obj = {
            #       "last_report_timestamp": datetime.datetime.now()}
            # else:
            #    self.state_obj = {"last_infer_ebic_timestamp": datetime.datetime.now()}

            if util.read_fast_from_app_config_to_property(["detectors", ElectricBicycleEnteringEventDetector.__name__],
                                                          'EnableSendConfirmedEbEnteringMsgToKafka'):
                if eb_entering_event_alarms and len(eb_entering_event_alarms) > 0:
                    confirmedmsg = "Vehicle|#|TwoWheeler" + "|TwoWheeler|confirmed"
                    self.sendMessageToKafka(confirmedmsg)

            t2 = time.time()
            after_infer_used_time = (t2 - t1) * 1000
            self.logger.debug("board: {}, used time after infer: {}ms".format(
                self.timeline.board_id, str(after_infer_used_time))[:5])
        if self.close_alarm and self.alarm_raised:
            self.close_alarm = False
            self.alarm_raised = False
            self.logger.debug("board: {}, close the ebike alarm".format(self.timeline.board_id))
            eb_entering_event_alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                                   event_alarm.EventAlarmPriority.CLOSE,
                                                                   "close the EBike alarm", "007"))
        return eb_entering_event_alarms

    def __process_infer_result__(self, timeline_item_original_timestamp, edge_board_confidence,
                                 infer_server_ebic_confid, story=0, image_str=""):
        event_alarms = []
        ebike_confid_threshold = util.read_fast_from_app_config_to_property(
            ["detectors", ElectricBicycleEnteringEventDetector.__name__],
            'ebic_confid')
        self.sendEBikeConfirmToLocal(
            infer_server_ebic_confid, ebike_confid_threshold)
        if infer_server_ebic_confid >= ebike_confid_threshold:
            if self.notifyEbIncomingAndCheckIfThrottleNeeded(infer_server_ebic_confid):
                self.logger.debug("board: {}, Throttled a high confid: {} eb event".format(self.timeline.board_id,
                                                                                           infer_server_ebic_confid))
                return []

        # if infer_server_ebic_confid >= 0.25 and abs(story) == 1:
        if self.raiseTheAlarm(infer_server_ebic_confid, ebike_confid_threshold) and abs(story) == 1:
            self.__fire_on_property_changed_event_to_subscribers__("E-bic Entering",
                                                                   {"detail": "there's a EB incoming"})
            self.alarm_raised = True
            self.close_alarm = False
            self.statistics_logger.debug("{} | {} | {}".format(
                self.timeline.board_id,
                "ElectricBicycleEnteringEventDetector_rasie_eb_alarm",
                ""
                ))
            event_alarms.append(
                event_alarm.EventAlarm(self, timeline_item_original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                       "detected electric-bicycle entering elevator with board confid: {}, server confid: {}".format(
                                           edge_board_confidence, infer_server_ebic_confid), "007", {}, image_str))
        else:
            self.statistics_logger.debug("{} | {} | {}".format(
                self.timeline.board_id,
                "ElectricBicycleEnteringEventDetector_sink_eb_alarm",
                "reason: low infer confid: {}, or not in story 1 or -1: {}".format(infer_server_ebic_confid, story)
                ))
            self.logger.debug(
                "      board: {}, sink this eb detect as server gives low confid: {}, "
                "or not in story 1 or -1, current storey is:{} ".format(
                    self.timeline.board_id,
                    infer_server_ebic_confid, story))
            # self.sendMessageToKafka("sink this eb detect. infer server gives low confidence:[]".format(
            # infer_server_ebic_confid))

        return event_alarms

    def save_sample_image(self, image_file_full_name, original_utc_timestamp: datetime, infered_class,
                          infer_server_ebic_confid, full_base64_image_file_text, dst_file_name_prefix=""):
        image_sample_path = os.path.join(
            ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH, self.timeline.board_id)
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
                                               :4] + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + "___" + self.timeline.board_id + ".jpg"))

        if full_base64_image_file_text and len(full_base64_image_file_text) > 1:
            temp_full_image = Image.open(io.BytesIO(
                base64.decodebytes(full_base64_image_file_text.encode('ascii'))))
            temp_full_image.save(os.path.join(
                image_sample_path,
                str(infer_server_ebic_confid) + "___full_image__" + self.timeline.board_id + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + ".jpg"))

    def raiseTheAlarm(self, infer_result, ebike_confid_threshold):
        keep_ebike_confid_threshold = util.read_fast_from_app_config_to_property(
            ["detectors", ElectricBicycleEnteringEventDetector.__name__],
            'keep_ebic_confid')
        result = False
        # 推理结果是电车，更新成功判断为电车的时间，如果enter_time 跟exit_time都有值时说明是新一轮的电车入梯
        # 如果enter_time有值而exit_time无值则认为是同一轮,只更新latest_infer_success
        if infer_result >= ebike_confid_threshold:
            self.ebike_state["latest_infer_success"] = datetime.datetime.now()

            if self.ebike_state["enter_time"] != "" and self.ebike_state["exit_time"] == "":
                result = False
                self.logger.info(
                    "board: {}, sink this eb detect as it's the same ebike, enter_time:{}, infer_result:{}".format(
                        self.timeline.board_id, self.ebike_state["enter_time"],
                        infer_result))
            elif self.needRaiseAlarm():
                self.ebike_state["enter_time"] = datetime.datetime.now()
                self.ebike_state["exit_time"] = ""
                result = True
                self.logger.info("board: {}, ebike entering at:{},latest_infer_success:{}".
                                 format(self.timeline.board_id,
                                        self.ebike_state["enter_time"],
                                        self.ebike_state["latest_infer_success"]))
            else:
                self.logger.info("board: {}, sink ebike entering,latest_infer_success:{}".
                                 format(self.timeline.board_id,
                                        self.ebike_state["latest_infer_success"]))
        # 成功上生成过电车告警且当前推理结果大于保留电车的阈值限制，则不认为出梯
        elif keep_ebike_confid_threshold != 0 and infer_result >= keep_ebike_confid_threshold and \
                self.ebike_state["enter_time"] != "":
            self.ebike_state["latest_infer_success"] = datetime.datetime.now()
            self.logger.info("board: {}, keep the eb enter state, enter_time:{}, infer_result:{}".format(
                self.timeline.board_id, self.ebike_state["enter_time"], infer_result))
        # 推理为非电车，如果距上次推理成功已有一段时间，那么则认为电动车出梯了
        else:
            time_to_keep_successful_infer_result = util.read_fast_from_app_config_to_property(
                ["detectors", ElectricBicycleEnteringEventDetector.__name__],
                'time_to_keep_successful_infer_result')
            temp_time = datetime.datetime.now()
            if self.ebike_state["latest_infer_success"] == "":
                self.ebike_state["enter_time"] = ""
                self.ebike_state["exit_time"] = ""
            elif self.ebike_state["latest_infer_success"] != "" and (
                    temp_time - self.ebike_state[
                "latest_infer_success"]).total_seconds() > time_to_keep_successful_infer_result:
                self.ebike_state["latest_infer_success"] = ""
                self.ebike_state["exit_time"] = temp_time
                self.close_alarm = True
                self.sendMessageToKafka(("|TwoWheeler|confirmedExit"))
                self.logger.info("board: {},ebike exit at:{}".format(
                    self.timeline.board_id, temp_time))
        return result

    def sendMessageToKafka(self, message):
        if util.read_fast_from_app_config_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
            return
        try:
            config_item = [i for i in self.timeline.configures if i["code"] == "kqzt"]
            config_kqzt = 1 if len(config_item) == 0 else int(config_item[0]["value"])
            if self.timeline.board_id in self.timeline.target_borads or config_kqzt == 0:
                # self.logger.info("board:{}is in the target list".format(self.timeline.board_id))
                return
            if not self.blockDoorEnabled(self.timeline.board_id):
                self.logger.info("board:{}.block door is disabled from web".format(self.timeline.board_id))
                return
            # self.logger.info("------------------------board:{} is not in the target list".format(self.timeline.board_id))
            # producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
            #                         value_serializer=lambda x: dumps(x).encode('utf-8'))
            self.logger.info("board:{} send message:{} to kafka".format(self.timeline.board_id, message))
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

    # avoid a single spike eb event mis-trigger alarm, set to 1 for disable the feature
    # we noticed some cases that like a bicycle entering, most of the frames can be infered as bicycle which is correct,
    # but sometimes, a Single frame can be infered as eb, which is wrong, here is trying to avoid this case
    def notifyEbIncomingAndCheckIfThrottleNeeded(self, confirmed_confid):
        self.infer_confirmed_eb_history_list.append(
            {"infer_time": datetime.datetime.now()})
        # remove multiple expired items at once from a list
        expired_items = [i for i in self.infer_confirmed_eb_history_list if
                         (datetime.datetime.now() - i[
                             "infer_time"]).total_seconds() > ElectricBicycleEnteringEventDetector.THROTTLE_Window_Time_By_Sec]
        self.infer_confirmed_eb_history_list = [
            i for i in self.infer_confirmed_eb_history_list if i not in expired_items]
        if confirmed_confid >= 0.99:
            # since the infer is so sure, then let it go through and not to throttle.
            return False
        if len(self.infer_confirmed_eb_history_list) < ElectricBicycleEnteringEventDetector.THROTTLE_Window_Depth:
            return True
        return False

    def sendEBikeConfirmToLocal(self, infer_result, config_result):
        surrived_items = [i for i in self.local_ebike_infer_result_list if
                          (datetime.datetime.now() - i["infer_time"]).total_seconds() < 5]
        if len(surrived_items) == 0 and infer_result == 0 and len(self.local_ebike_infer_result_list) > 0 and \
                self.local_ebike_infer_result_list[0]["confirm_send"] == True:
            self.local_ebike_infer_result_list[0]["confirm_send"] = False
            # self.logger.debug("send ebike confirm exit-1")
            self.close_alarm = True
            self.sendMessageToKafka(("|TwoWheeler|confirmedExit"))

        # if there's no ebike items from local in 5 seconds, will think the ebike is out
        if len(surrived_items) == 0:
            self.local_ebike_infer_result_list = []

        if infer_result == 0:
            return
        if len(self.local_ebike_infer_result_list) == 0:
            # self.logger.debug("send ebike confirm")
            self.sendMessageToKafka(
                ("Vehicle|#|TwoWheeler" + "|TwoWheeler|confirmed"))
            self.local_ebike_infer_result_list.append(
                {"infer_result": infer_result, "confirm_send": True, "infer_time": datetime.datetime.now()})
            return
        local_ebike_count = util.read_fast_from_app_config_to_property(
            ["detectors", ElectricBicycleEnteringEventDetector.__name__],
            'local_ebic_count')
        self.local_ebike_infer_result_list.append(
            {"infer_result": infer_result, "confirm_send": False, "infer_time": datetime.datetime.now()})
        if len(self.local_ebike_infer_result_list) >= local_ebike_count and self.local_ebike_infer_result_list[0][
            "confirm_send"]:
            confirmed_result = [
                i for i in self.local_ebike_infer_result_list if i["infer_result"] >= config_result]
            config_result_85 = [
                i for i in self.local_ebike_infer_result_list if i["infer_result"] >= 0.85]
            # since the infer is so sure, then let it go through and not to cancel confirm
            if len(config_result_85) > 0:
                self.local_ebike_infer_result_list[0]["confirm_send"] = False
                return
            if len(confirmed_result) < 2:
                self.local_ebike_infer_result_list[0]["confirm_send"] = False
                # self.logger.debug("send ebike confirm exit")
                self.close_alarm = True
                self.sendMessageToKafka(("|TwoWheeler|confirmedExit"))

    # 本地识别到的电车在进行识别后，如果已经进行了max_infer_count(比如10次)次推理都不是电动车，那么在max_infer_time的
    # 时间段内将不再生成告警
    # infering_stage__how_many_continuous_non_eb_see_then_enter_silent_period配置为0时作废该逻辑
    def needRaiseAlarm(self):
        result = False

        max_infer_count = util.read_fast_from_app_config_to_property(
            ["detectors", ElectricBicycleEnteringEventDetector.__name__],
            'infering_stage__how_many_continuous_non_eb_see_then_enter_silent_period')

        # 配置为0时作废该逻辑
        if max_infer_count == 0:
            return True

        # 推理次数还未达到配置的最大次数，不影响原有逻辑
        if len(self.local_ebike_infer_result_list) < max_infer_count:
            return True

        ebic_confid = util.read_fast_from_app_config_to_property(
            ["detectors", ElectricBicycleEnteringEventDetector.__name__],
            'ebic_confid')
        surrived_items = self.local_ebike_infer_result_list[0:max_infer_count]
        surrived_items = [i for i in surrived_items if i["infer_result"] >= ebic_confid]
        # 如果前max_infer_count中有推理成功的电车，则不影响原有逻辑
        if len(surrived_items) > 0:
            return True

        # 如果第一次推理到现在已经超过配置的最大推理时间，则不影响原有逻辑
        infering_stage__when_see_many_non_eb_then_enter_silent_period_duration = util.read_fast_from_app_config_to_property(
            ["detectors", ElectricBicycleEnteringEventDetector.__name__],
            'infering_stage__when_see_many_non_eb_then_enter_silent_period_duration')
        if (datetime.datetime.now() - self.local_ebike_infer_result_list[0][
            "infer_time"]).total_seconds() >= infering_stage__when_see_many_non_eb_then_enter_silent_period_duration:
            return True

        return result

    # Web获取阻梯是否开启
    def blockDoorEnabled(self, deviceId: str):
        try:
            get_all_board_ids_response = requests.get(
                "https://api.glfiot.com/api/apiduijie/getliftcontrol?serialNo=" + deviceId,
                headers={'Content-type': 'application/json', 'Accept': 'application/json'},
                timeout=1)
            if get_all_board_ids_response.status_code != 200:
                return False
            json_result = get_all_board_ids_response.json()
            if "data" in json_result:
                return json_result["data"]
            return False
        except:
            self.logger.exception("get lift control via web API (...) rasised an exception:")
            return False

