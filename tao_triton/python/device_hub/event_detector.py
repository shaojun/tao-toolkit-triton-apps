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


class EventDetectorBase:
    def __init__(self, logging):
        self.event_listeners = []
        self.state_obj: dict = None
        self.timeline = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return None

        return None

    def subscribe_on_property_changed(self, event_listener):
        """
        registering the caller as a subscriber to listen the inner property changed event
        @param event_listener: the caller must has the function of on_property_changed_event_handler(..., ...).
        """
        self.event_listeners.append(event_listener)

    def on_property_changed_event_handler(self, src_detector, property_name: str, data):
        """
        will be called if the inner property changed event get fired
        @param src_detector: the detector who firing the inner property changed event
        @param property_name: the name of the property has the data changed on
        @param data: the data with the event
        """
        pass

    def __fire_on_property_changed_event_to_subscribers__(self, property_name: str, data):
        """
        fire an inner property changed event to all subscribers
        @param property_name: the name of the property has the data changed on
        @param data: the data with the event
        """
        if self.event_listeners:
            for el in self.event_listeners:
                el.on_property_changed_event_handler(self, property_name, data)

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        return None

    # def where(self, items: List[TimelineItem], last_count: int):
    #     return items[-last_count:]


#
# 门的基本状态改变检测： 已开门， 已关门
class DoorStateChangedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger(
            "doorStateChangedEventDetectorLogger")
        self.electricBicycleEnteringEventDetector_instance = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        for det in event_detectors:
            if det.__class__.__name__ == ElectricBicycleEnteringEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                self.electricBicycleEnteringEventDetector_instance = det
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == ElectricBicycleEnteringEventDetector.__name__:
            # handle event from ElectricBicycleEnteringEventDetector
            pass

    # def get_timeline_item_filter(self):
    #     def filter(timeline_items: List[TimelineItem]):
    #         return [i for i in timeline_items if
    #                 i.type == TimelineItemType.OBJECT_DETECT and
    #                 not i.consumed and "Vehicle|#|DoorWarningSign" in i.raw_data]
    #
    #     return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None

        # sample for show how to retrieve other detector's state obj
        if self.electricBicycleEnteringEventDetector_instance is not None:
            ebike_detector_state_obj = self.electricBicycleEnteringEventDetector_instance.state_obj
            if ebike_detector_state_obj is not None and "last_infer_ebic_timestamp" in ebike_detector_state_obj:
                last_infer_ebic_timestamp = ebike_detector_state_obj[
                    "last_infer_ebic_timestamp"]

        recent_items = [i for i in filtered_timeline_items if
                        not i.consumed and
                        i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                        (datetime.datetime.now() - i.local_timestamp).total_seconds() < 3]

        target_msg_original_timestamp_str = "" if len(
            recent_items) == 0 else recent_items[-1].original_timestamp_str
        person_items = [i for i in filtered_timeline_items if
                        "Person|#" in i.raw_data and i.original_timestamp_str == target_msg_original_timestamp_str]

        target_items = []
        count = 0
        for item in reversed(recent_items):
            if item.original_timestamp_str != target_msg_original_timestamp_str:
                target_msg_original_timestamp_str = item.original_timestamp_str
                count = count + 1
            if count < 3:
                target_items.append(item)

        # target_items_last = [i for i in filtered_timeline_items if
        #                     i.original_timestamp_str == target_msg_original_timestamp_str]

        # target_items = recent_items
        # self.logger.debug(
        #    "total length of recent items:{}, target items:{}".format(len(recent_items), len(target_items)))
        """
        for item in target_items:
            if "Vehicle|#|TwoWheeler" in item.raw_data or "|TwoWheeler|confirmed" in item.raw_data:
                self.logger.debug("Item in door state detect. e-bike. original time:{}".format(
                    item.original_timestamp_str))
            elif "Vehicle|#|gastank" in item.raw_data:
                self.logger.debug("Item in door state detect. gastank. original time:{}".format(
                    item.original_timestamp_str))
            else:
                self.logger.debug(
                    "item in door state detect,board:{},board_msg_id:{}, item type:{},raw_data:{},original time:{}".format(
                        item.timeline.board_id,
                        item.board_msg_id,
                        item.item_type,
                        item.raw_data,
                        item.original_timestamp_str))
        """
        if len(target_items) > 0:
            log_item = target_items[0]
            self.logger.debug(
                "item in door state detect,board:{},board_msg_id:{}, item type:{},raw_data:{},original time:{}".format(
                    log_item.timeline.board_id,
                    log_item.board_msg_id,
                    log_item.item_type,
                    log_item.raw_data,
                    log_item.original_timestamp_str))
        hasPereson = "Y" if len(person_items) > 0 else "N"
        for ri in target_items:
            if ri.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_door_state": "OPEN",
                                 "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "Vehicle|#|DoorWarningSign" in ri.raw_data:
                new_state_obj = {"last_door_state": "CLOSE",
                                 "last_state_timestamp": str(datetime.datetime.now())}
                break
        if not new_state_obj:
            new_state_obj = {"last_door_state": "OPEN",
                             "last_state_timestamp": str(datetime.datetime.now())}

        # store it back
        self.state_obj = new_state_obj
        if (last_state_obj and last_state_obj["last_door_state"] != new_state_obj[
            "last_door_state"]) or last_state_obj is None:
            self.__fire_on_property_changed_event_to_subscribers__(
                "door_state",
                {"last_state": "None" if last_state_obj is None else last_state_obj["last_door_state"],
                 "new_state": new_state_obj["last_door_state"],
                 "hasPerson": hasPereson})
            code = "DOOROPEN" if new_state_obj["last_door_state"] == "OPEN" else "DOORCLOSE"
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.INFO,
                                       "Door state changed to: {},human_inside:{}".format(
                                           new_state_obj["last_door_state"], hasPereson), code,
                                       {"human_inside": hasPereson})]
        return None


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
        self.global_statistics_logger = logging.getLogger(
            "globalStatisticsLogger")
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
            try:
                # self.global_statistics_logger.debug("{} | {} | {}".format(self.timeline.board_id, "1st_model_pre_infer",""))
                raw_infer_results = tao_client.callable_main(['-m', 'elenet_four_classes_230722_tao',
                                                              '--mode', 'Classification',
                                                              '-u', self.infer_server_ip_and_port,
                                                              '--output_path', './',
                                                              temp_cropped_image_file_full_name])
                infered_class = raw_infer_results[0][0]['infer_class_name']
                infer_server_current_ebic_confid = raw_infer_results[0][0]['infer_confid']
                self.global_statistics_logger.debug("{} | {} | {}".format(
                    self.timeline.board_id, 
                    "1st_model_post_infer",
                    "infered_class: {}, infered_confid: {}, used_time: {}".format(
                        infered_class,
                        infer_server_current_ebic_confid,
                        str(time.time() - infer_start_time)[:5])))
            except Exception as e:
                self.global_statistics_logger.exception("{} | {} | {}".format(
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
                                                                'ebic_confid'):
                    # self.global_statistics_logger.debug("{} | {} | {}".format(
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
                    self.global_statistics_logger.debug("{} | {} | {}".format(
                        self.timeline.board_id,
                        "2nd_model_post_infer",
                        "infered_confid: {}, infered_confid: {}".format(second_infer_infered_class,second_infer_infered_confid) 
                    ))
                    if second_infer_infered_class == 'eb':
                        self.global_statistics_logger.debug("{} | {} | {}".format(
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
                            self.global_statistics_logger.debug("{} | {} | {}".format(
                                self.timeline.board_id,
                                "2nd_model_post_infer_confirm_eb",
                                "non_eb_confid: {}".format(non_eb_confid)
                                ))
                            pass
                        else:
                            self.global_statistics_logger.debug("{} | {} | {}".format(
                                self.timeline.board_id,
                                "2nd_model_post_infer_sink_eb",
                                "non_eb_confid: {}".format(non_eb_confid)
                                ))
                            self.logger.debug(
                                "      board: {}, sink this eb(from 4 class model) due to detect as non_eb(from 2 class model) with confid: {}"
                                .format(
                                    self.timeline.board_id,
                                    non_eb_confid))
                            self.save_sample_image(temp_cropped_image_file_full_name, item.original_timestamp,
                                       infered_class, infer_server_current_ebic_confid, full_base64_image_file_text, "2nd_m_non_eb_{}___".format(str(non_eb_confid)[
                                               :4]))
                            # gives a fake class of eb and confid to make sure the following logic can be executed
                            # following logic will treat this as eb with low confid and will not trigger alarm, but other logic will still be executed
                            infered_class = 'electric_bicycle'
                            infer_server_current_ebic_confid = 0.119
            except Exception as e:
                self.global_statistics_logger.exception("{} | {} | {}".format(
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
            self.global_statistics_logger.debug("{} | {} | {}".format(
                self.timeline.board_id,
                "ElectricBicycleEnteringEventDetector_rasie_eb_alarm",
                ""
                ))
            event_alarms.append(
                event_alarm.EventAlarm(self, timeline_item_original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                       "detected electric-bicycle entering elevator with board confid: {}, server confid: {}".format(
                                           edge_board_confidence, infer_server_ebic_confid), "007", {}, image_str))
        else:
            self.global_statistics_logger.debug("{} | {} | {}".format(
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


#
# 煤气罐检测告警（煤气罐入梯)
class GasTankEnteringEventDetector(EventDetectorBase):
    SAVE_IMAGE_SAMPLE_ROOT_FOLDER_PATH = "gastank_image_samples"
    Enable_SAVE_IMAGE_SAMPLE = True

    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger("gasTankEnteringEventDetectorLogger")
        self.need_close_alarm = False

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.state_obj = {"last_infer_timestamp": None}
        self.timeline = timeline
        pass

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

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        is_enabled = util.read_fast_from_app_config_to_board_control_level(
            ["detectors", 'GasTankEnteringEventDetector', 'FeatureSwitchers'], self.timeline.board_id)
        event_alarms = []
        gas_tank_items = [i for i in filtered_timeline_items if "Vehicle|#|gastank" in i.raw_data]

        # self.logger.debug("board:{},len gas tank item:{}, last_infer_timestamp:{} ".
        #                  format(self.timeline.board_id, len(gas_tank_items), self.state_obj["last_infer_timestamp"]))
        if self.need_close_alarm and self.state_obj and "last_infer_timestamp" in self.state_obj and self.state_obj[
            "last_infer_timestamp"] and \
                len(gas_tank_items) == 0 and \
                (datetime.datetime.now() - self.state_obj["last_infer_timestamp"]).total_seconds() > 20:
            self.logger.debug("board:{} close gas tank entering".format(self.timeline.board_id))
            # self.state_obj["last_infer_timestamp"] = None
            self.need_close_alarm = False
            self.sendMessageToKafka("|TwoWheeler|confirmedExit")
            event_alarms.append(
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.CLOSE,
                                       "Close gas tank alarm", "0021"))
            return event_alarms

        if len(gas_tank_items) > 0:
            self.state_obj["last_infer_timestamp"] = datetime.datetime.now()

        # if self.state_obj and "last_infer_timestamp" in self.state_obj and self.state_obj["last_infer_timestamp"]:
        if self.need_close_alarm:
            for item in gas_tank_items:
                item.consumed = True
            return event_alarms

        if len(gas_tank_items) > 0:
            item = gas_tank_items[-1]
            for item in gas_tank_items:
                item.consumed = True
            '''
            if self.state_obj and "last_infer_timestamp" in self.state_obj and self.state_obj["last_infer_timestamp"]:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_infer_timestamp"]).total_seconds()
                # if last_report_time_diff <= 60 * 60 * 24:

                if last_report_time_diff <= 10:
                    return event_alarms
                    # continue
            '''
            # self.logger.debug(
            #     "timeline_item in gas tank detect raw data:{}".format(item.raw_data))

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

            # is_enabled = util.read_fast_from_app_config_to_board_control_level(
            #    ["detectors", 'GasTankEnteringEventDetector', 'FeatureSwitchers'], self.timeline.board_id)
            # is_enabled = True
            if is_enabled:
                self.global_statistics_logger.debug("{} | {} | {}".format(
                    self.timeline.board_id,
                    "GasTankEnteringEventDetector_confirm_gastank",
                    "data: {}".format("")
                    ))
                # self.state_obj["last_infer_timestamp"] = datetime.datetime.now()
                self.need_close_alarm = True
                self.sendMessageToKafka("Vehicle|#|TwoWheeler" + "|TwoWheeler|confirmed")
                # self.sendMessageToKafka("test info")
                event_alarms.append(
                    event_alarm.EventAlarm(self, item.original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                           "detected gas tank entering elevator with board confid: {}".format(
                                               edge_board_confidence)))
            self.logger.debug("borad:{} raise gas tank entering alarm".format(self.timeline.board_id))
        return event_alarms

    def sendMessageToKafka(self, message):
        try:
            config_item = [i for i in self.timeline.configures if i["code"] == "kqzt"]
            config_kqzt = 1 if len(config_item) == 0 else int(config_item[0]["value"])
            if self.timeline.board_id in self.timeline.target_borads or config_kqzt == 0:
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


#
# 遮挡门告警	判断电梯发生遮挡门事件 1.电梯内有人 2.门处于打开状态 3.电梯静止
# 将长时间开门告警结合在此处，如果没人，门打开超过一定时间则报长时间开门
class BlockingDoorEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger("blockingDoorEventDetector")

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，如果它是处于关闭状态，那么就不用判断遮挡门
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now()}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            # and "Person|#" in i.raw_data
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT or
                         (
                                 i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data)) and
                    (datetime.datetime.now() - i.local_timestamp).total_seconds() < 120]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        door_state = None
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        last_state_object = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_object = self.state_obj["last_notify_timestamp"]
        # 提交告警结束
        if last_state_object and self.state_obj["alarm_code"] == "002" and self.canCloseTheAlarm(
                filtered_timeline_items):
            self.state_obj["last_notify_timestamp"] = None
            self.state_obj["alarm_code"] = ""
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "002")]
        elif last_state_object and self.state_obj["alarm_code"] == "008" and self.canCloseLongTimeOpen():
            self.state_obj["last_notify_timestamp"] = None
            self.state_obj["alarm_code"] = ""
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "008")]

        object_items = [i for i in filtered_timeline_items if
                        i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        if len(object_items) == 0:
            return None

        # 如果已经产生过告警且没有结束，那么就不再处理
        if last_state_object:
            return None

        if not door_state or door_state["new_state"] != "OPEN":
            return None

        door_open_time_diff = (datetime.datetime.now(
        ) - door_state["notify_time"]).total_seconds()
        config_item_1 = [i for i in self.timeline.configures if i["code"] == "zdm"]
        config_time_1 = 90 if len(config_item_1) == 0 else int(config_item_1[0]["value"])
        # 如果收到的开门状态时间还很短，那么不作遮挡判断
        if abs(door_open_time_diff) < config_time_1:
            return None
        # 是否符合长时间开门
        config_item = [i for i in self.timeline.configures if i["code"] == "csjkm"]
        config_time = 120 if len(config_item) == 0 else int(config_item[0]["value"])
        can_raise_door_openned_long_time = False if abs(door_open_time_diff) < config_time else True

        can_raise_door_blocked = True
        person_timeline_items = [i for i in filtered_timeline_items if
                                 i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                                 "Person|#" in i.raw_data]
        if not len(person_timeline_items) > 0:
            can_raise_door_blocked = False
        # 电梯内没有人 不判断遮挡
        if len(person_timeline_items) > 0:
            # can_raise_door_blocked = False
            # return None
            latest_person_item = person_timeline_items[-1]
            detect_person_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                       latest_person_item.original_timestamp).total_seconds()
            # 最近一次识别到人类已经有一段时间，那么可以认为电梯内没人
            if detect_person_time_diff > 3:
                can_raise_door_blocked = False
            # return None

        '''
        last_state_object = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_object = self.state_obj["last_notify_timestamp"]
        if last_state_object:
            notify_time_diff = (datetime.datetime.now() -
                                last_state_object).total_seconds()
            if notify_time_diff < 90:
                return None
        '''

        speed_timeline_items = [i for i in filtered_timeline_items if
                                i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                                "speed" in i.raw_data]
        # 未能获取到电梯速度，无法判断电梯是否静止
        if not len(speed_timeline_items) > 2:
            can_raise_door_blocked = False
            # return None

        new_state_object = None
        if len(speed_timeline_items) > 2:
            latest_speed_item = speed_timeline_items[-1]
            latest_speed_item_time_diff = (datetime.datetime.now(
                datetime.timezone.utc) - latest_speed_item.original_timestamp).total_seconds()
            third_speed_item = speed_timeline_items[-3]
            third_speed_item_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                          third_speed_item.original_timestamp).total_seconds()
            if abs(latest_speed_item_time_diff) < 4 and latest_speed_item.raw_data["speed"] < 0.1 and \
                    abs(third_speed_item_time_diff) < 6 and third_speed_item.raw_data["speed"] < 0.2:
                new_state_object = datetime.datetime.now()
            else:
                can_raise_door_blocked = False

        if can_raise_door_blocked:
            self.logger.debug("board:{},遮挡门告警中，开门时长为{}s".format(self.timeline.board_id, door_open_time_diff))
            self.state_obj["last_notify_timestamp"] = new_state_object
            self.state_obj["alarm_code"] = "002"
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "电梯门被遮挡", "002")]
        elif can_raise_door_openned_long_time:
            alarms = [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                             event_alarm.EventAlarmPriority.ERROR,
                                             "长时间处于开门状态 - 门开着超过一定时间的故障, 最近一次门状态转变为'开'的时间为: {}, 且持续时间 >= {}秒".format(
                                                 door_state["notify_time"].strftime(
                                                     "%d/%m/%Y %H:%M:%S"),
                                                 (datetime.datetime.now() - door_state["notify_time"]).total_seconds()),
                                             "008")]
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            self.state_obj["alarm_code"] = "008"
            return alarms
        return None

    # 遮挡门告警发生后，电梯开始运行或关门状态持续30S
    def canCloseTheAlarm(self, filtered_timeline_items):
        speed_items = [i for i in filtered_timeline_items if
                       i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                       "speed" in i.raw_data and (datetime.datetime.now(datetime.timezone.utc) -
                                                  i.original_timestamp).total_seconds() < 6]
        if len(speed_items) > 2 and speed_items[0].raw_data["speed"] > 0.3 and \
                speed_items[-1].raw_data["speed"] > 0.3:
            return True

        if self.state_obj and "door_state" in self.state_obj and \
                self.state_obj["door_state"]["new_state"] == "CLOSE" and \
                (datetime.datetime.now() - self.state_obj["door_state"]["notify_time"]).total_seconds() > 30:
            return True

        return False

    # 是否可以关闭长时：关门解除
    def canCloseLongTimeOpen(self):
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]
        if last_state_obj and (datetime.datetime.now() - last_state_obj).total_seconds() > 10 and \
                self.state_obj and "door_state" in self.state_obj and \
                self.state_obj["door_state"]["new_state"] == "CLOSE":
            return True
        return False


#
# 困人	判断电梯发生困人事件
class PeopleStuckEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger("peopleStuckEventDetector")
        self.state_obj = {"last_notify_timestamp": None}
        self.last_report_close_time = None

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "has_person": data["hasPerson"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and
                    ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and (
                            "Person|#" in i.raw_data in i.raw_data or "#|DoorWarningSign" in i.raw_data)) or
                     (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED))]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        if last_state_obj and ((datetime.datetime.now() - last_state_obj).total_seconds() > 600 or
                               self.canCloseAlarm(filtered_timeline_items)):
            self.state_obj["last_notify_timestamp"] = None
            self.last_report_close_time = datetime.datetime.now()
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "001")]

        if last_state_obj:
            return None

        if self.last_report_close_time and (datetime.datetime.now() - self.last_report_close_time).total_seconds() < 60:
            return None

        door_state = None
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        # 关门的时候没有人，那么不进行困人判断
        if not door_state or door_state["new_state"] == "OPEN" or door_state["has_person"] == "N":
            return None
        # if door_state["last_state"] == "OPEN" and (
        #        datetime.datetime.now() - door_state["notify_time"]).total_seconds() < 20:
        #    return None
        # 如果门关上还未超出2分钟则不认为困人
        if door_state["new_state"] == "CLOSE" and (
                datetime.datetime.now(datetime.timezone.utc) - door_state["notify_time"]).total_seconds() < 90:
            return None
        '''
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        new_state_obj = None
        if last_state_obj and "last_report_timestamp" in last_state_obj:
            last_report_time_diff = (
                datetime.datetime.now() - last_state_obj["last_report_timestamp"]).total_seconds()
            # 如果短时间内上报过
            if last_report_time_diff < 120:
                return None
        '''
        new_state_obj = None
        # "Person|#"
        person_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in
                                          i.raw_data
                                          and (door_state["notify_time"] - i.original_timestamp).total_seconds() < 0]
        # 电梯内没人
        if len(person_filtered_timeline_items) < 20:
            return None
        # object_person = None
        object_person = person_filtered_timeline_items[0]
        if (datetime.datetime.now(datetime.timezone.utc) - object_person.original_timestamp).total_seconds() < 50:
            return None
        # self.logger.debug("困人检测中共发现{}人，最早目标出现在:{}".format(len(person_filtered_timeline_items),
        #                                                                  object_person.original_timestamp_str))
        object_person = None
        for person in reversed(person_filtered_timeline_items):
            latest_time_diff = (
                    datetime.datetime.now(datetime.timezone.utc) - person.original_timestamp).total_seconds()
            person_door_close_time_diff = (
                    door_state["notify_time"] - person.original_timestamp).total_seconds()
            if latest_time_diff < 3 and person_door_close_time_diff < 0:
                object_person = person
            break

        # 如果在3秒内没有发现有人，那么不认为有困人
        if not object_person:
            return None

        # 如果最近的人出现时没有门标则不认为困人
        door_signs = [i for i in filtered_timeline_items if
                      i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "#|DoorWarningSign" in i.raw_data
                      and object_person.original_timestamp == i.original_timestamp]
        if len(door_signs) == 0:
            return None

        kunren_sj_item = [i for i in self.timeline.configures if i["code"] == "krsj"]
        kunren_sj = 90 if len(kunren_sj_item) == 0 else int(kunren_sj_item[0]["value"])
        if (datetime.datetime.now(datetime.timezone.utc) - person_filtered_timeline_items[0].
                original_timestamp).total_seconds() < kunren_sj:
            return None
        # speed
        speed_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                         board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if len(speed_filtered_timeline_items) == 0:
            return None
        is_quiescent = True
        for item in speed_filtered_timeline_items:
            time_diff = (datetime.datetime.now(
                datetime.timezone.utc) - item.original_timestamp).total_seconds()
            # 如果120秒内电梯有在运动那么不认为困人
            if time_diff <= kunren_sj and abs(item.raw_data["speed"]) > 0.3:
                is_quiescent = False
        if is_quiescent:
            new_state_obj = {"people_stuck": "stuck",
                             "last_report_timestamp": datetime.datetime.now()}

        if new_state_obj:
            self.last_report_close_time = None
            self.logger.debug("board:{} people count:{},latest person:{}".format(self.timeline.board_id,
                                                                                 len(person_filtered_timeline_items),
                                                                                 object_person.original_timestamp_str))
            # store it back, and it will be passed in at next call
            self.state_obj["last_notify_timestamp"] = new_state_obj["last_report_timestamp"];
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "{}发现有人困在电梯内".format(
                                           new_state_obj["last_report_timestamp"].strftime("%d/%m/%Y %H:%M:%S")),
                                       "001")]
        return None

    # 门开 或 轿厢内没人
    def canCloseAlarm(self, filtered_timeline_items):
        door_state = None
        # self.logger.debug("board:{} Check if can close the alarm which uploaded at:{}".
        #                  format(self.timeline.board_id, str(self.state_obj["last_notify_timestamp"])))
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        # 门开
        if not door_state or door_state["new_state"] == "OPEN":
            return True

        filtered_person_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in
                                          i.raw_data
                                          and (door_state["notify_time"] - i.original_timestamp).total_seconds() < 0]
        # 电梯内没人
        if len(filtered_person_timeline_items) < 20:
            return True

        object_person = None
        object_person = filtered_person_timeline_items[-1]
        if (datetime.datetime.now(datetime.timezone.utc) - object_person.original_timestamp).total_seconds() > 10:
            return True
        self.logger.debug("board:{} can not close the alarm which uploaded at:{}".format(self.timeline.board_id, str(
            self.state_obj["last_notify_timestamp"])))
        return False


#
# 超速	判断电梯发生超速故障
class ElevatorOverspeedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_speed_state": "normal", "last_speed": 0, "last_notify_time_stamp": None}
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data and
                         (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 10)]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        if len(filtered_timeline_items) < 6:
            return None

        if self.state_obj and "last_notify_time_stamp" in self.state_obj and self.state_obj["last_notify_time_stamp"]:
            time_diff = (datetime.datetime.now() - self.state_obj["last_notify_time_stamp"]).total_seconds()
            if self.canCloseAlarm(filtered_timeline_items, time_diff):
                self.state_obj["last_notify_time_stamp"] = None
                return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                               event_alarm.EventAlarmPriority.CLOSE, "", "0020")]
            else:
                # 有正在进行的告警
                return None
        if len(filtered_timeline_items) < 6:
            return None

        surrived_item = [i for i in filtered_timeline_items if
                         (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                          "speed" in i.raw_data and "maxVelocity" in i.raw_data and
                          abs(i.raw_data["speed"]) > i.raw_data["maxVelocity"] and abs(i.raw_data["speed"] > 2.5) and
                          abs(i.raw_data["speed"] < 4))]

        if len(surrived_item) < 6:
            return None

        new_state_obj = {"last_speed_state": "overspeed", "last_speed": filtered_timeline_items[-1].raw_data["speed"],
                         "last_notify_time_stamp": datetime.datetime.now()}
        self.state_obj = new_state_obj

        return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
            datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯超速，当前速度：{}m/s".format(new_state_obj["last_speed"]), "0020")]

    # 运行速度恢复15秒才解除
    def canCloseAlarm(self, filtered_timeline_items, time_diff):
        if time_diff < 15:
            return False

        if time_diff > 120:
            return True

        if len(filtered_timeline_items) < 2:
            return False
        surrived_item = [i for i in filtered_timeline_items if
                         (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                          "speed" in i.raw_data and "maxVelocity" in i.raw_data and
                          abs(i.raw_data["speed"]) > i.raw_data["maxVelocity"])]
        if len(surrived_item) < 3:
            return True
        return False


#
# 反复开关门	判断电梯发生反复开关门故障
class DoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.alarms_to_fire = []
        self.timeline = None
        self.logger = logging.getLogger("doorRepeatlyOpenAndCloseEventDetectorLogger")

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_state_changed_times": [], "last_report_time": None}
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return [i for i in timeline_items if
                    not i.consumed
                    # and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data)
                    and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data
                    and (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 8]

        return filter

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 一次开关门应包括开门跟关门
            if property_name == "door_state":
                if self.state_obj and "last_state_changed_times" in self.state_obj:
                    self.state_obj["last_state_changed_times"].append(
                        datetime.datetime.now())
                    last_state_changed_times: List[datetime.datetime] = self.state_obj["last_state_changed_times"]
                    self.logger.debug("total len of last_state_changed item:{}".format(
                        len(last_state_changed_times)))

                    last_report_time = None if not ("last_report_time" in self.state_obj) else self.state_obj[
                        "last_report_time"]
                    # 已上报过且未结束不再上报
                    if last_report_time:
                        return
                    if len(last_state_changed_times) >= 6:
                        total_time_gap = (last_state_changed_times[-1] - last_state_changed_times[-3]
                                          + last_state_changed_times[-3] - last_state_changed_times[
                                              -5]).total_seconds()
                        config_item = [i for i in self.timeline.configures if i["code"] == "ffkgm"]
                        config_time = 15 if len(config_item) == 0 else int(config_item[0]["value"])
                        if total_time_gap <= config_time:
                            '''
                            last_report_time = None if not ("last_report_time" in self.state_obj) else self.state_obj[
                                "last_report_time"]

                            # 已上报过且未结束不再上报
                            if last_report_time:
                                return

                                report_time_diff = (datetime.datetime.now() - last_report_time).total_seconds()
                                if report_time_diff < 20:
                                    self.logger.debug(
                                        "last report time:{},time_diff:{}".format(last_report_time, report_time_diff))
                                    return
                            '''
                            # self.state_obj["last_state_changed_times"] = []
                            self.alarms_to_fire.append(event_alarm.EventAlarm(
                                self,
                                datetime.datetime.fromisoformat(
                                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                event_alarm.EventAlarmPriority.ERROR,
                                "反复开关门 - 判断电梯发生反复开关门故障,3次开关门的总间隔<=20秒,最近三次 开/关门 从近到远时间分别为: {}  {}  {}".format(
                                    last_state_changed_times[-1].strftime(
                                        "%H:%M:%S"),
                                    last_state_changed_times[-3].strftime(
                                        "%H:%M:%S"),
                                    last_state_changed_times[-5].strftime("%H:%M:%S"))))
                            # self.state_obj["last_state_changed_times"] = []
                        elif len(self.state_obj["last_state_changed_times"]) > 6:
                            self.state_obj["last_state_changed_times"] = self.state_obj["last_state_changed_times"][-6:]
                else:
                    self.state_obj = {"last_state_changed_times": [
                        datetime.datetime.now()]}
                    self.logger.debug(
                        "total len of last_state_changed item:{}".format(len(self.state_obj)))
                # door state values are: OPEN, CLOSE, None
                # data["last_state"]
                # data["new_state"]
                # pass

    # 电梯不运行或15s内电梯状态改变5次
    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # 电梯没有速度信息 不判断反复开关
        if not len(filtered_timeline_items) > 3:
            self.alarms_to_fire.clear()
            return None
        latest_speed_item = filtered_timeline_items[-1]
        detect_speed_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                  latest_speed_item.original_timestamp).total_seconds()
        # 最近一次收到速度信息已经有一段时间，那么无法判断电梯是否停止
        if detect_speed_time_diff > 4:
            self.alarms_to_fire.clear()
            return None

        # 可以关闭，发送结束信息至云端
        if self.state_obj and "last_report_time" in self.state_obj and \
                self.state_obj["last_report_time"] and self.canCloseAlarm(filtered_timeline_items):
            self.alarms_to_fire.clear()
            self.state_obj["last_state_changed_times"] = []
            self.state_obj["last_report_time"] = None
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "004")]

        # 电梯运动不判断反复开关
        if filtered_timeline_items[-3].raw_data["speed"] > 0.3 and \
                filtered_timeline_items[-1].raw_data["speed"] > 0.3:
            self.alarms_to_fire.clear()
            return None

        '''
        if len(self.alarms_to_fire) > 0:
            alarm = self.alarms_to_fire[0]
            target_persons = [i for i in filtered_timeline_items if (alarm.original_utc_timestamp - i.original_timestamp
                                                                     ).total_seconds() > 5 and
                              (alarm.original_utc_timestamp - i.original_timestamp).total_seconds() < 10]
            if len(target_persons) == 0:
                self.alarms_to_fire.clear()
                return None

            self.state_obj["last_report_time"] = datetime.datetime.now()
            self.logger.debug(
                "反复开关门判断中出现人：{},raw:{}".format(target_persons[-1].original_timestamp_str,
                                                          target_persons[-1].raw_data))
        '''
        alarms = self.alarms_to_fire
        if len(self.alarms_to_fire) > 0:
            self.state_obj["last_report_time"] = datetime.datetime.now()
        self.alarms_to_fire = []
        return alarms

    # 电梯运行或30秒门的状态没有改变
    def canCloseAlarm(self, filtered_timeline_items):
        # 电梯运行
        if len(filtered_timeline_items) > 3 and filtered_timeline_items[-3].raw_data["speed"] > 0.3 and \
                filtered_timeline_items[-1].raw_data["speed"] > 0.3:
            return True

        if self.state_obj and "last_state_changed_times" in self.state_obj and \
                len(self.state_obj["last_state_changed_times"]) > 0:
            door_state_change_obj = self.state_obj["last_state_changed_times"]
            if (datetime.datetime.now() - door_state_change_obj[-1]).total_seconds() > 30:
                return True
        return False


#
# 剧烈运动	判断轿厢乘客剧烈运动
class PassagerVigorousExerciseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed and
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data) or
                       (
                               i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "acceleration" in i.raw_data))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj and "last_notify_timestamp" in last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 120:
                return None

        object_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        acceleration_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                                board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        detect_person = None
        for item in reversed(object_filtered_timeline_items):
            if (datetime.datetime.now(datetime.timezone.utc) - item.original_timestamp).total_seconds() < 2:
                detect_person = item
            break

        for item in reversed(acceleration_filtered_timeline_items):
            if item.raw_data["acceleration"] > item.raw_data["maxAcceleration"] and detect_person:
                new_state_obj = {"last_state": "shock", "acceleration": item.raw_data["acceleration"],
                                 "last_notify_timestamp": datetime.datetime.now()}
            break
        if not new_state_obj:
            return None
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "剧烈运动，当前加速度: {}".format(new_state_obj["acceleration"]), "005")]
        return None


#
# 运行中开门	判断电梯发生运行中开门故障
class DoorOpeningAtMovingEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}
        for ed in event_detectors:
            if ed.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                ed.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        self.logger.debug(
            "{} is notified by event from: {} for property: {} with data: {}".format(
                self.__class__.__name__,
                src_detector.__class__.__name__,
                property_name,
                str(data)))
        # 记录上次开门时间
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                property_name == "door_state":
            self.state_obj["door_state"] = {"state": data["new_state"],
                                            "time": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data) or
                    (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                     (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 3)
                    ]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        # 如果告警超过20分钟，可能存在问题暂时关闭
        if last_state_obj and (self.canCloseAlarm(filtered_timeline_items) or (
                datetime.datetime.now() - last_state_obj).total_seconds() > 1200):
            self.state_obj["last_notify_timestamp"] = None
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.CLOSE, "", "0022")]

        if last_state_obj:
            return None

        objects = [i for i in filtered_timeline_items if i.item_type ==
                   board_timeline.TimelineItemType.OBJECT_DETECT]
        if len(objects) == 0:
            return None

        door_state = None if not (self.state_obj and "door_state" in self.state_obj) \
            else self.state_obj["door_state"]
        # 如果没有开门事件，则不认为有在运动中开门的事发生
        if not door_state or door_state["state"] != "OPEN":
            return None

        # 如果开门状态维持超过2分钟，暂时不做运行中开门预警，可能开关门状态不对
        if (datetime.datetime.now(datetime.timezone.utc) - door_state["time"]).total_seconds() > 120:
            return None

        new_state_obj = None
        '''
        if last_state_obj:
            last_notify_time_diff = (
                datetime.datetime.now() - last_state_obj).total_seconds()
            if last_notify_time_diff < 10:
                return None
        '''
        temp_speed = 0.0
        speedItems = [i for i in filtered_timeline_items if
                      i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                      (i.original_timestamp - door_state["time"]).total_seconds() > 0 and
                      abs(i.raw_data["speed"]) > 0.5]
        if len(speedItems) < 5:
            return None

        new_state_obj = datetime.datetime.now()
        '''
        for speed_timeline_item in reversed(speedItems):
            temp_speed = abs(speed_timeline_item.raw_data["speed"])
            door_open_speed_time_diff = (speed_timeline_item.original_timestamp -
                                         door_state["time"]).total_seconds()
            # 获取到的速度值在开门之后
            if temp_speed > 1 and door_open_speed_time_diff > 0:
                new_state_obj = datetime.datetime.now()
            break
        '''
        # 将报警时间存入状态字典
        self.state_obj["last_notify_timestamp"] = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯门在运行中打开,当前速度为:{}m/s".format(temp_speed), "0022")]
        return None

    # 门关上或者电梯停止运行
    def canCloseAlarm(self, filtered_timeline_items):
        if self.state_obj and "door_state" in self.state_obj and self.state_obj["door_state"]["state"] == "CLOSE":
            return True

        speedItems = [i for i in filtered_timeline_items if
                      i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if len(speedItems) < 5:
            return False

        if abs(speedItems[-1].raw_data["speed"]) < 0.3 and abs(speedItems[-2].raw_data["speed"]) < 0.3 and \
                abs(speedItems[-3].raw_data["speed"]) < 0.3 and abs(speedItems[-4].raw_data["speed"]) < 0.3 and abs(
            speedItems[-5].raw_data["speed"]) < 0.3:
            return True
        return False


#
# 急停	判断电梯发生急停故障
class ElevatorSuddenlyStoppedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_notify_timestamp": datetime.datetime.now()}
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data)]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = None
        new_state_obj = None
        if self.state_obj:
            last_state_obj = self.state_obj
        if last_state_obj:
            last_notify_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_notify_time_diff < 300:
                return None

        if len(filtered_timeline_items) < 2:
            return None

        latest_speed_item = filtered_timeline_items[-1]
        previous_speed_item = filtered_timeline_items[-2]

        speed_change_time_diff = (previous_speed_item.original_timestamp -
                                  latest_speed_item.original_timestamp).total_seconds()
        if abs(speed_change_time_diff) > 3 or latest_speed_item.raw_data["speed"] > 4.9:
            return None
        if latest_speed_item and abs(latest_speed_item.raw_data["speed"]) < 0.1 \
                and previous_speed_item and abs(previous_speed_item.raw_data["speed"]) > 1.5:
            new_state_obj = {"current_speed": latest_speed_item.raw_data["speed"],
                             "current_time": latest_speed_item.original_timestamp_str,
                             "previous_speed": previous_speed_item.raw_data["speed"],
                             "previous_time": previous_speed_item.original_timestamp_str,
                             "last_notify_timestamp": datetime.datetime.now()}
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯发生急停,速度从{}的{}m/s在{}变成{}".format(
                                           new_state_obj["previous_time"],
                                           new_state_obj["previous_speed"],
                                           new_state_obj["current_time"],
                                           new_state_obj["current_speed"]), "006")]
        return None


#
# 长时间开门	门开着超过一定时间
class DoorOpenedForLongtimeEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.last_door_open_state_time = None
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {"last_notify_timestamp": None}
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                           (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 3)]
            return result

        return filter

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            if property_name == "door_state":
                # door state values are: OPEN, CLOSE, None
                if data["new_state"] == "OPEN":
                    self.last_door_open_state_time = datetime.datetime.now()
                else:
                    self.last_door_open_state_time = None

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        if len(filtered_timeline_items) == 0:
            return None
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]
            self.logger.debug("board {}， has notified at:{}, door open state:{}".
                              format(self.timeline.board_id, str(self.state_obj["last_notify_timestamp"]),
                                     str(self.last_door_open_state_time)))
        # state_obj 不为空则上报过长时间开门， 而且 last_door_open_state_time为None则认为门关上，告警结束
        if last_state_obj and (
                datetime.datetime.now() - last_state_obj).total_seconds() > 10 and self.last_door_open_state_time is None:
            self.state_obj["last_notify_timestamp"] = None

            result_alarms = [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                    event_alarm.EventAlarmPriority.CLOSE, "长时间开门告警结束", "008")]
            return result_alarms

        # state_obj 不为空怎代表上报过，如果此时的门状态依然为开，那么不重复告警
        if self.state_obj and self.state_obj["last_notify_timestamp"] and self.last_door_open_state_time:
            return None

        config_item = [i for i in self.timeline.configures if i["code"] == "csjkm"]
        config_time = 120 if len(config_item) == 0 else int(config_item[0]["value"])
        if self.last_door_open_state_time \
                and (datetime.datetime.now() - self.last_door_open_state_time).total_seconds() >= config_time:
            alarms = [event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                "长时间处于开门状态 - 门开着超过一定时间的故障, 最近一次门状态转变为'开'的时间为: {}, 且持续时间 >= {}秒".format(
                    self.last_door_open_state_time.strftime(
                        "%d/%m/%Y %H:%M:%S"),
                    (datetime.datetime.now() - self.last_door_open_state_time).total_seconds()))]
            # self.last_door_open_state_time = None
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            return alarms
        return None


#
# 无人高频运行	轿厢内没人, 连续运行15分钟以上
class ElevatorMovingWithoutPeopleInEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        # 最近一次电梯静止时间
        self.state_obj = {"latest_jingzhi_time_stamp": datetime.datetime.now()}
        self.state_obj = {"latest_yunxing_time_stamp": datetime.datetime.now()}
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态. 无人高频运动的情况电梯门应该是一直处于关闭状态
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now()}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data)
                           or (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                               and "speed" in i.raw_data and (datetime.datetime.now(
                                          datetime.timezone.utc) - i.original_timestamp).total_seconds() < 6))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        speed_timeline_item = [i for i in filtered_timeline_items if
                               i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if len(speed_timeline_item) > 3 and abs(speed_timeline_item[-1].raw_data["speed"]) > 0.3 and abs(
                speed_timeline_item[-2].raw_data["speed"]) > 0.3 and \
                abs(speed_timeline_item[-3].raw_data["speed"]) > 0.3:
            self.state_obj["latest_yunxing_time_stamp"] = datetime.datetime.now()

        # 如果连续三次的速度都近似静止，则认为电梯静止
        if len(speed_timeline_item) > 3 and abs(speed_timeline_item[-1].raw_data["speed"]) < 0.3 and abs(
                speed_timeline_item[-2].raw_data["speed"]) < 0.3 and \
                abs(speed_timeline_item[-3].raw_data["speed"]) < 0.3:
            self.state_obj["latest_jingzhi_time_stamp"] = datetime.datetime.now()

        last_state_object = None if not (self.state_obj and "last_notify_timestamp" in self.state_obj) \
            else self.state_obj["last_notify_timestamp"]

        if last_state_object and self.canCloseAlarm():
            self.state_obj["last_notify_timestamp"] = None
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "0019")]

        # 如果告警还没结束
        if last_state_object:
            return None

        door_state_object = None if not (self.state_obj and "door_state" in self.state_obj) \
            else self.state_obj["door_state"]
        if (not door_state_object) or door_state_object["new_state"] == "OPEN":
            return None
        door_close_time_diff = (datetime.datetime.now(
        ) - door_state_object["notify_time"]).total_seconds()
        # 如果门处于关闭状态小于5分钟那么对于电梯要完成无人反复上下可能性不大
        if door_close_time_diff < 300:
            return None
        '''
        if last_state_object:
            last_report_time_diff = (
                datetime.datetime.now() - last_state_object).total_seconds()
            if last_report_time_diff < 120:
                return None
        '''
        person_timeline_item = [i for i in filtered_timeline_items if i.item_type ==
                                board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data]
        if len(person_timeline_item) > 0:
            latest_person_object = person_timeline_item[-1]
            latest_detect_person_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                              latest_person_object.original_timestamp).total_seconds()
            if latest_detect_person_time_diff < 600:
                return None

        if (datetime.datetime.now() - self.state_obj["latest_jingzhi_time_stamp"]).total_seconds() > 900:
            if not self.state_obj:
                self.state_obj = {}
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.WARNING,
                                           "电梯无人，高频运行{}秒".format((datetime.datetime.now() - self.state_obj[
                                               "latest_jingzhi_time_stamp"]).total_seconds()), "0019")]

        return None

    # 30s内电梯不运行
    def canCloseAlarm(self):
        if (datetime.datetime.now() - self.state_obj["latest_yunxing_time_stamp"]).total_seconds() > 30:
            return True
        return False


#
# 电梯温度过高
class TemperatureTooHighEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        # super().prepare(event_detectors)
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                      and "temperature" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = self.state_obj
        new_state_obj = None
        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_temperature_state": "normal",
                                 "last_notify_timestamp": datetime.datetime.now()}
                break
            elif "temperature" in item.raw_data:
                state = "normal"
                if not isinstance(item.raw_data["temperature"], str) and abs(item.raw_data["temperature"]) > 50:
                    state = str(item.raw_data["temperature"])
                new_state_obj = {"last_temperature_state": state,
                                 "last_notify_timestamp": datetime.datetime.now()}
                break
        if not new_state_obj:
            new_state_obj = {"last_temperature_state": "unknown",
                             "last_notify_timestamp": datetime.datetime.now()}
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 10:
                return None
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj and new_state_obj["last_temperature_state"] != 'unknown' and \
                new_state_obj["last_temperature_state"] != "normal":
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯温度异常，当前温度: {}".format(
                                           new_state_obj["last_temperature_state"]),
                                       "0013")]
        return None


#
# 震动 短时间内，发生正向和反向加速度过大
class ElevatorShockEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                      and "acceleration" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 5:
                return None

        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = None
                break
            elif "acceleration" in item.raw_data and "maxAcceleration" in item.raw_data:
                if abs(item.raw_data["acceleration"]) > abs(item.raw_data["maxAcceleration"]):
                    new_state_obj = {"current_acceleration": item.raw_data["acceleration"],
                                     "configured_max_acceleration": item.raw_data["maxAcceleration"],
                                     "last_notify_timestamp": datetime.datetime.now()}
                break
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯发生震动:当前加速度:{}m/s2，配置的最大加速度为：{}m/s2".format(
                                           new_state_obj["current_acceleration"],
                                           new_state_obj["configured_max_acceleration"]),
                                       "005")]
        return None


# 电梯拥堵
class ElevatorJamsEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger("elevatorJamsEventDetectorLogger")

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.state_obj = {"last_notify_timestamp": None}
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                      and "Person|#" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj and self.state_obj["last_notify_timestamp"]:
            last_state_obj = self.state_obj["last_notify_timestamp"]

        new_state_obj = None
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj).total_seconds()
            if last_report_time_diff > 30:
                self.state_obj["last_notify_timestamp"] = None
                return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                               event_alarm.EventAlarmPriority.CLOSE, "电梯拥堵结束", "0011")]
            else:
                return None

        # 识别到的人的记录小于10条，那么不算拥挤
        if not len(filtered_timeline_items) > 10:
            return None
        latest_person_object = filtered_timeline_items[-1]

        target_persons = [i for i in filtered_timeline_items if i.board_msg_id == latest_person_object.board_msg_id
                          and i.original_timestamp_str == latest_person_object.original_timestamp_str]
        self.logger.debug("当前人数{}".format(len(target_persons)))
        config_item = [i for i in self.timeline.configures if i["code"] == "dtyd"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        # store it back, and it will be passed in at next call
        if len(target_persons) > config_count:
            if not self.state_obj:
                self.state_obj = {}
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯拥堵:当前人数:{}".format(len(target_persons)),
                                       "0011")]
        return None


#
# 电梯里程
class ElevatorMileageEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                                   and "speed" in i.raw_data
                           ) or (
                                   i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = None
        new_state_obj = None
        alarms = []
        if self.state_obj and "elevator_state" in self.state_obj:
            last_state_obj = self.state_obj["elevator_state"]
        if len(filtered_timeline_items) > 0:
            speed_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                             board_timeline.TimelineItemType.SENSOR_READ_SPEED]
            storey_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                              board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
            if len(speed_filtered_timeline_items) > 0 and len(storey_filtered_timeline_items) > 0 and \
                    speed_filtered_timeline_items[-1].board_msg_id == storey_filtered_timeline_items[-1].board_msg_id:
                current_speed_item = speed_filtered_timeline_items[-1]
                current_storey_item = storey_filtered_timeline_items[-1]
                if last_state_obj:
                    state_len = len(last_state_obj)
                    if len(last_state_obj) == 1:
                        if current_speed_item.raw_data["speed"] != 0:
                            last_state_obj.append({"speed": current_speed_item.raw_data["speed"],
                                                   "storey": current_storey_item.raw_data["storey"],
                                                   "timestamp": datetime.datetime.now(datetime.timezone.utc)})
                        else:
                            last_state_obj[0]["timestamp"] = datetime.datetime.now(
                                datetime.timezone.utc)
                    elif len(last_state_obj) == 2:
                        if current_speed_item.raw_data["speed"] == 0:
                            last_state_obj.append({"speed": current_speed_item.raw_data["speed"],
                                                   "storey": current_storey_item.raw_data["storey"],
                                                   "timestamp": datetime.datetime.now(datetime.timezone.utc)})
                        else:
                            last_state_obj[1]["timestamp"] = datetime.datetime.now(
                                datetime.timezone.utc)
                else:
                    last_state_obj = [{"speed": current_speed_item.raw_data["speed"],
                                       "storey": current_storey_item.raw_data["storey"],
                                       "timestamp": datetime.datetime.now(datetime.timezone.utc)}]
        if last_state_obj and len(last_state_obj) == 3:
            floor_count = abs(
                last_state_obj[0]["storey"] - last_state_obj[2]["storey"])
            passenger_count = 0
            persons = [i for i in filtered_timeline_items if i.item_type ==
                       board_timeline.TimelineItemType.OBJECT_DETECT and
                       (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 3]
            if len(persons) > 0:
                latest_person_item = persons[-1]
                targets = [i for i in persons if i.original_timestamp_str ==
                           latest_person_item.original_timestamp_str]
                passenger_count = len(targets)
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.INFO,
                "floor_count:{},passenger_count:{},start_date:{},end_date:{}".format(floor_count, passenger_count,
                                                                                     last_state_obj[0]["timestamp"],
                                                                                     last_state_obj[2]["timestamp"]),
                "TRIP", {"floor_count": str(floor_count),
                         "passenger_count": str(passenger_count),
                         "start_date": str(last_state_obj[0]["timestamp"]),
                         "end_date": str(last_state_obj[2]["timestamp"])}))
            last_state_obj.pop(0)
            last_state_obj.pop(0)
        self.state_obj["elevator_state"] = last_state_obj
        return alarms


#
# 电梯运行状态 上行、下行 停止 所在楼层
class ElevatorRunningStateEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []
        self.door_state = ""

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        # self.logger.debug(
        #     "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
        #         self.timeline.board_id,
        #         self.__class__.__name__,
        #         src_detector.__class__.__name__,
        #         property_name,
        #         str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.door_state = data["new_state"]

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      # and (datetime.datetime.now() - i.local_timestamp).total_seconds() <= 10
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                                   and "speed" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                                   and "Person|#" in i.raw_data
                           ) or i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT)]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        # 下行时速度>0 上行时速度<0
        if len(filtered_timeline_items) > 0:
            person_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                     board_timeline.TimelineItemType.OBJECT_DETECT]
            speed_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                    board_timeline.TimelineItemType.SENSOR_READ_SPEED]
            storey_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                     board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
            sensor_detect_person_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT]
            sensor_detect_person = False
            if len(sensor_detect_person_items) > 0:
                sensor_detect_person = sensor_detect_person_items[-1].raw_data["detectPerson"]
            hasPerson = "Y" if len(person_timeline_items) > 0 else "N"
            person_count = 0
            if len(person_timeline_items) > 0:
                latest_person_object = filtered_timeline_items[-1]
                target_persons = [i for i in filtered_timeline_items if
                                  i.board_msg_id == latest_person_object.board_msg_id
                                  and i.original_timestamp_str == latest_person_object.original_timestamp_str]
                person_count = len(target_persons)

            code = ""
            speed = 0.00 if not len(speed_timeline_items) > 0 else speed_timeline_items[-1].raw_data["speed"]
            acceleration = 0.00 if not len(speed_timeline_items) > 0 else speed_timeline_items[-1].raw_data[
                "acceleration"]

            # LIFTUP LIFTDOWN
            if len(speed_timeline_items) > 4:
                last_speed_object = speed_timeline_items[-1]
                previous_speed_oject = speed_timeline_items[-2]
                speed = last_speed_object.raw_data["speed"]
                if abs(last_speed_object.raw_data["speed"]) < 0.2 and \
                        abs(previous_speed_oject.raw_data["speed"]) < 0.2:
                    code = "LIFTSTOP"
                # 如果电梯在运动，比较楼层的变化判断向上、向下
                elif not (abs(last_speed_object.raw_data["speed"]) < 0.2):
                    storey_latest = storey_timeline_items[-1].raw_data["storey"]
                    storey_last_fifth = storey_timeline_items[-5].raw_data["storey"]
                    code = "LIFTDOWN" if storey_latest < storey_last_fifth else "LIFTUP"

            storey = 0 if not len(storey_timeline_items) > 0 else storey_timeline_items[-1].raw_data["storey"]
            pressure = 0 if not len(storey_timeline_items) > 0 else storey_timeline_items[-1].raw_data["pressure"]
            if last_state_object and "code" in last_state_object:
                if last_state_object["code"] == code and last_state_object["floor"] == storey:
                    # last_state_object["hasPerson"] == hasPerson:
                    return None
            if code != "":
                self.state_obj = {"code": code, "floor": storey, "hasPerson": hasPerson,
                                  "time_stamp": datetime.datetime.now()}
                # if last_state_object and last_state_object["code"] == code and last_state_object["floor"] == \
                #        storey and last_state_object["hasPerson"] == hasPerson:
                if last_state_object and "time_stamp" in last_state_object:
                    report_diff = (datetime.datetime.now(
                    ) - last_state_object["time_stamp"]).total_seconds()
                    if report_diff < 2:
                        return None
                time = datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat())
                '''
                data = {"human_inside": hasPerson, "floor_num": str(storey)} if code == "LIFTSTOP" \
                    else {"human_inside": hasPerson, "floor_num": str(storey), "speed": str(speed)}
                '''
                guang_dian = 1 if storey == 1 else 0
                # model7 识别人数 model8:轿厢顶人体感应 model10:光电感值 model4:液晶屏楼层 model2门状态
                data = {"model1": storey, "model2": self.door_state, "model3": code, "model4": storey,
                         "model5": pressure,
                         "model6": speed, "model7": person_count, "model8": sensor_detect_person,
                         "model9": acceleration,
                         "model10": guang_dian,
                         "createdDate": str(time),
                         "liftId": self.timeline.liftId}

                alarms.append(event_alarm.EventAlarm(
                    self, time,
                    event_alarm.EventAlarmPriority.INFO,
                    "human_inside:{},floor_num:{},speed:{}".format(
                        hasPerson, storey, speed),
                    "general_data", data))
        return alarms


#
# 陀螺仪故障 1，获取不到加速度 2，连续5次读到的加速度值相同（正常情况下，即使不动每次读取的值也不一样）
class GyroscopeFaultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                      and "raw_acceleration" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 4:
            return None
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            if last_report_time_diff < 600:
                return None
        config_item = [i for i in self.timeline.configures if i["code"] == "tlygz"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        target_timeline_items = filtered_timeline_items[-1 * config_count:]
        # 如果读取到的加速度值一直是零/相同
        latest_raw_acceleration = target_timeline_items[-1].raw_data["raw_acceleration"]
        result_timeline_items = [i for i in target_timeline_items if i.raw_data["raw_acceleration"] ==
                                 latest_raw_acceleration]
        if len(result_timeline_items) == config_count:
            msg = "获取不到加速度" if latest_raw_acceleration == 0.000 else "连续五次获取到的加速度值相同"
            self.state_obj = {"code": "TLYJG", "message": msg,
                              "time_stamp": datetime.datetime.now()}
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                msg,
                "TLYJG"))
        return alarms


#
# 光电感故障 1，获取不到温度值/气压值 2，连续5次读到的气压值相同（正常情况下，即使不动每次读取的值也不一样）
# 3，收到光电信号时，气压值连续3次不在1楼标定的气压范围内
class PressureFaultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 4:
            return None
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            if last_report_time_diff < 600:
                return None
        config_item = [i for i in self.timeline.configures if i["code"] == "qyjgz"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        target_timeline_items = filtered_timeline_items[-1 * config_count:]
        # 如果读取到的气压值一直是零/相同
        latest_raw_pressure = target_timeline_items[-1].raw_data["pressure"]
        result_timeline_items = [i for i in target_timeline_items if i.raw_data["pressure"] ==
                                 latest_raw_pressure]
        if len(result_timeline_items) == config_count:
            msg = "获取不到气压值" if latest_raw_pressure == 0.000 else "连续五次获取到的气压值相同"
            new_state_object = {"code": "TLYJG", "message": msg,
                                "time_stamp": datetime.datetime.now()}
        result_timeline_items = [
            i for i in target_timeline_items if i.raw_data["temperature"] == 0.00]
        if len(result_timeline_items) == config_count:
            msg = "气压计获取不到温度"
            if new_state_object:
                new_state_object["message"] = new_state_object["message"] + ";" + msg
            else:
                new_state_object = {
                    "code": "QYJGZ", "message": msg, "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "QYJGZ"))
        return alarms


# 边缘计算超过1天时长获取的光电感信号一直为1
# 光电感应故障，直接在边缘端判断
class ElectricSwitchFaultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_ELECTRIC_SWITCH]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None
        target_timeline_item = filtered_timeline_items[-1]
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            # 光电故障6小时报一次，不用太频繁
            if last_report_time_diff < 21600:
                target_timeline_item.consumed = True
                return None
        target_timeline_item.consumed = True
        new_state_object = {
            "code": "GDGXSYC", "message": "光电感已超过1天未触发", "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "GDGXSYC"))
        return alarms


# 边缘板子软件包更新结果
class UpdateResultEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.UPDATE_RESULT]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None
        target_timeline_item = filtered_timeline_items[-1]
        target_timeline_item.consumed = True
        if target_timeline_item.raw_data['update']:
            return None
        new_state_object = {"code": "UPGRADEERROR",
                            "message": target_timeline_item.raw_data['description'],
                            "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                new_state_object["message"],
                new_state_object["code"]))
        return alarms


# 边缘板子离线
class DeviceOfflineEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        # 启动时假设边缘板子在线
        self.state_obj = {
            "detect_device_time_stamp": datetime.datetime.now(datetime.timezone.utc)}

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None
        if last_state_object and "notify_time" in last_state_object:
            notify_time_diff = (datetime.datetime.now(
            ) - last_state_object["notify_time"]).total_seconds()
            if notify_time_diff < 600:
                return None

        target_timeline_item = [i for i in filtered_timeline_items if
                                not i.consumed
                                and (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED or
                                     i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT)]
        if target_timeline_item and len(target_timeline_item) > 0:
            latest_speed_item = target_timeline_item[-1]
            self.state_obj["detect_device_time_stamp"] = latest_speed_item.original_timestamp

        time_diff = (datetime.datetime.now(datetime.timezone.utc) - self.state_obj[
            "detect_device_time_stamp"]).total_seconds()
        config_item = [i for i in self.timeline.configures if i["code"] == "sblx"]
        config_count = 6 if len(config_item) == 0 else int(config_item[0]["value"])
        if time_diff < config_count * 60:
            return None
        new_state_object = {"code": "0023",
                            "message": "边缘设备离线，最近一次获取到边缘设备信息的时间为{}".format(
                                self.state_obj["detect_device_time_stamp"].strftime("%d/%m/%Y %H:%M:%S")),
                            "time_stamp": datetime.datetime.now()}

        if new_state_object:
            self.state_obj["notify_time"] = datetime.datetime.now()
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                new_state_object["message"],
                new_state_object["code"]))
        return alarms


# 光电感应故障,检测到电梯顶部有人
class DetectPersonOnTopEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        config_item = [i for i in self.timeline.configures if i["code"] == "jdyr"]
        config_count = 5 if len(config_item) == 0 else int(config_item[0]["value"])
        if not len(filtered_timeline_items) > config_count:
            return None
        # target_timeline_item = filtered_timeline_items[-1]
        # target_timeline_item.consumed = True
        detect_person = "Y"
        description = "{}测到轿顶有人".format(
            datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        for item in reversed(filtered_timeline_items):
            item.consumed = True
            if "detectPerson" in item.raw_data and not item.raw_data["detectPerson"]:
                detect_person = "N"
                description = "{}未检测到轿顶有人".format(
                    datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

        # if "detectPerson" in target_timeline_item.raw_data:
        #    detect_person = "Y" if target_timeline_item.raw_data["detectPerson"] else "N"
        #    description = "未检测到轿顶有人" if detect_person == "N" else "检测到轿顶有人进入"
        new_state_object = {"code": "HUMANONTOP", "detectPerson": detect_person, "message": description,
                            "time_stamp": datetime.datetime.now()}
        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            # 检测顶部有人5分钟报一次
            if last_report_time_diff < 300 and "detectPerson" in last_state_object and \
                    last_state_object["detectPerson"] == detect_person:
                return None
        if new_state_object:
            self.state_obj = new_state_object
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "HUMANONTOP", {"human_on_top": detect_person}))
        return alarms


# ISAP 检测到摄像头被遮挡事件
class DetectCameraBlockedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        self.state_obj = {}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.CAMERA_BLOCKED]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        alarms = []
        last_state_object = self.state_obj
        new_state_object = None
        if not len(filtered_timeline_items) > 0:
            return None

        target_timeline_item = filtered_timeline_items[-1]
        target_timeline_item.consumed = True
        if "cameraBlocked" in target_timeline_item.raw_data:
            if target_timeline_item.raw_data["cameraBlocked"]:
                description = "摄像头被遮挡，检测到的时间为：{}".format(
                    target_timeline_item.local_utc_timestamp.strftime("%d/%m/%Y %H:%M:%S"))
                new_state_object = {"code": "009", "cameraBlocked": True, "message": description,
                                    "time_stamp": datetime.datetime.now()}
        if last_state_object:
            if new_state_object == None:
                self.state_obj = None
                # self.sendMessageToKafka("|TwoWheeler|confirmedExit")
                alarms.append(event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                                     event_alarm.EventAlarmPriority.CLOSE,
                                                     "close camera blocked alarm", "009"))
                return alarms
            else:
                return None

        if new_state_object:
            self.state_obj = new_state_object

            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                new_state_object["message"],
                "009"))
            '''
            self.sendMessageToKafka("Vehicle|#|TwoWheeler" + "|TwoWheeler|confirmed")
            alarms.append(
                event_alarm.EventAlarm(self,
                                       datetime.datetime.fromisoformat(
                                           datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "detected electric-bicycle due to camera block event", "007", {}))
            '''
            self.logger.debug("cameraBlocked boardId:{}".format(self.timeline.board_id))
        return alarms

    def sendMessageToKafka(self, message):
        try:
            config_item = [i for i in self.timeline.configures if i["code"] == "kqzt"]
            config_kqzt = 1 if len(config_item) == 0 else int(config_item[0]["value"])
            if self.timeline.board_id in self.timeline.target_borads or config_kqzt == 0:
                return
            # self.logger.info("------------------------board:{} is not in the target list".format(self.timeline.board_id))
            # producer = KafkaProducer(bootstrap_servers='msg.glfiot.com',
            #                         value_serializer=lambda x: dumps(x).encode('utf-8'))
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


# 门标
class DetectDoorWarningSignLostEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.alarms_to_fire = []

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner even
        """
        self.timeline = timeline
        # 启动时假设门标
        self.state_obj = {"detect_door_warning_sign_time_stamp": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            result = [i for i in timeline_items if
                      not i.consumed
                      and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "DoorWarningSign" in i.raw_data and
                      (datetime.datetime.now(datetime.timezone.utc) - i.original_timestamp).total_seconds() < 10]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        if len(filtered_timeline_items) > 0:
            self.state_obj["detect_door_warning_sign_time_stamp"] = filtered_timeline_items[-1].original_timestamp

        last_notify_time = None
        if self.state_obj and "notify_time" in self.state_obj and self.state_obj["notify_time"]:
            last_notify_time = self.state_obj["notify_time"]

        # 关闭告警
        if last_notify_time and len(filtered_timeline_items) > 5:
            self.state_obj["notify_time"] = None
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.CLOSE, "", "DOORWARNINGSIGN")]

        if last_notify_time:
            return None

        alarms = []
        config_item = [i for i in self.timeline.configures if i["code"] == "ffkgm"]
        config_time = 15 if len(config_item) == 0 else int(config_item[0]["value"])
        if ((datetime.datetime.now(datetime.timezone.utc) - self.state_obj[
            "detect_door_warning_sign_time_stamp"]).total_seconds() > config_time):
            self.state_obj["notify_time"] = datetime.datetime.now()
            alarms.append(event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.WARNING,
                "最近一次检测到门标的时间为：{}".format(
                    self.state_obj["detect_door_warning_sign_time_stamp"].strftime("%d/%m/%Y %H:%M:%S")),
                "DOORWARNINGSIGN"))
        return alarms