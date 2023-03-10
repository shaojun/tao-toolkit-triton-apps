import datetime
import os
import re
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
from tao_triton.python.device_hub import base64_tao_client


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
        self.logger = logging.getLogger(__name__)
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
        self.logger.debug(
            "total length of recent items:{}, target items:{}".format(len(recent_items), len(target_items)))
        for item in target_items:
            self.logger.debug(
                "item in door state detect,board:{},board_msg_id:{}, item type:{},raw_data:{},original time:{}".format(
                    item.timeline.board_id,
                    item.board_msg_id,
                    item.item_type,
                    item.raw_data,
                    item.original_timestamp_str))
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
                 "new_state": new_state_obj["last_door_state"]})
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
    SAVE_EBIC_IMAGE_SAMPLE_FOLDER_PATH = "ebic_image_samples"

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
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                            and "Vehicle|#|TwoWheeler" in i.raw_data) or
                           (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data
                            and abs((datetime.datetime.now(
                                       datetime.timezone.utc) - i.original_timestamp).total_seconds()) < 6))]
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
        self.logger.debug(
            "ElectricBicycleEnteringEventDetector object len:{},board:{},current_story:{}".format(
                len(object_filtered_timeline_items),
                self.timeline.board_id, current_story))
        for item in object_filtered_timeline_items:
            item.consumed = True
            if self.state_obj and "last_infer_ebic_timestamp" in self.state_obj:
                # we don't want to report too freq
                last_infer_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_infer_ebic_timestamp"]).total_seconds()
                self.logger.debug("last_infer_time_diff:{}".format(last_infer_time_diff))
                if last_infer_time_diff <= 4:
                    continue
            if self.state_obj and "last_report_timestamp" in self.state_obj:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_report_timestamp"]).total_seconds()
                self.logger.debug("last_report_time_diff:{}".format(last_report_time_diff))
                if last_report_time_diff <= 90:
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
            try:
                infer_results = base64_tao_client.infer(False, False, False,
                                                        "elenet_four_classes_tao", "",
                                                        1, "",
                                                        False, "192.168.66.149:8000", "HTTP", "Classification",
                                                        os.path.join(
                                                            os.getcwd(), "outputs"),
                                                        [cropped_base64_image_file_text])
            except:
                self.logger.exception("base64_tao_client.infer(...) rasised an exception:")
                return
            self.logger.debug("      board: {}, (localConf:{})infer_results: {}".format(
                self.timeline.board_id,
                edge_board_confidence, infer_results))
            temp = self.__process_infer_result__(
                item.original_timestamp, edge_board_confidence, full_base64_image_file_text, infer_results, True,
                current_story)
            eb_entering_event_alarms.extend(temp)
            if eb_entering_event_alarms and len(eb_entering_event_alarms) > 0:
                self.state_obj = {"last_report_timestamp": datetime.datetime.now()}
            else:
                self.state_obj = {"last_infer_ebic_timestamp": datetime.datetime.now()}
            # if eb_entering_event_alarms and len(eb_entering_event_alarms) > 0:
            # producer = KafkaProducer(bootstrap_servers='msg.glfiot.com')
            # confirmedmsg = item.raw_data + "|TwoWheeler|confirmed"
            # producer.send(self.timeline.board_id,
            #              bytes(confirmedmsg, 'utf-8'))
            # producer.flush(5)
            # producer.close()

        return eb_entering_event_alarms

    def __process_infer_result__(self, timeline_item_original_timestamp, edge_board_confidence,
                                 full_base64_image_file_text, infer_results, enable_save_sample_image=True, story=0):
        event_alarms = []
        infer_server_ebic_confid = 0
        # triton infer model classes list is defined as : classes = ['background', 'bicycle', 'electric_bicycle', 'people']
        # sample: (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(1)=bicycle, 0.4476(2)=electric_bicycle
        # the `0.4476(2)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
        # which is less than 50%.
        m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)', infer_results)
        if m and m.group(0):
            infer_server_ebic_confid = float(m.group(0))
            if infer_server_ebic_confid >= 0.4 and abs(story) == 1:
                self.__fire_on_property_changed_event_to_subscribers__("E-bic Entering",
                                                                       {"detail": "there's a EB incoming"})
                event_alarms.append(
                    event_alarm.EventAlarm(self, timeline_item_original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                           "detected electric-bicycle entering elevator with board confid: {}, server confid: {}".format(
                                               edge_board_confidence, infer_server_ebic_confid)))
            else:
                self.logger.debug(
                    "      board: {}, sink this eb detect due to infer server gives low confidence: {}, "
                    "or the elevator is not in story 1 or -1, current storey is:{} ".format(
                        self.timeline.board_id,
                        infer_results, story))
        else:
            self.logger.debug(
                "      board: {}, sink this eb detect due to infer server treat as non-eb class at all: {}".format(
                    self.timeline.board_id,
                    infer_results))
        if enable_save_sample_image:
            if not os.path.exists(ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_FOLDER_PATH):
                os.makedirs(
                    ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_FOLDER_PATH)
            file_name_timestamp_str = str(
                datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
            shutil.copyfile(os.path.join("temp_infer_image_files", "0.jpg"),
                            os.path.join(
                                ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_FOLDER_PATH,
                                str(infer_server_ebic_confid) + "___" + self.timeline.board_id + "___" + file_name_timestamp_str + ".jpg"))

            if full_base64_image_file_text and len(full_base64_image_file_text) > 1:
                temp_full_image = Image.open(io.BytesIO(
                    base64.decodebytes(full_base64_image_file_text.encode('ascii'))))
                temp_full_image.save(os.path.join(
                    ElectricBicycleEnteringEventDetector.SAVE_EBIC_IMAGE_SAMPLE_FOLDER_PATH,
                    str(infer_server_ebic_confid) + "___full_image__" + self.timeline.board_id + "___" + file_name_timestamp_str + ".jpg"))
        return event_alarms


#
# 煤气罐检测告警（煤气罐入梯)


class GasTankEnteringEventDetector(EventDetectorBase):
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
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            result = [i for i in timeline_items if
                      not i.consumed
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                      and "Vehicle|#|gastank" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """

        event_alarms = []
        for item in filtered_timeline_items:
            item.consumed = True
            if self.state_obj and "last_infer_timestamp" in self.state_obj:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_infer_timestamp"]).total_seconds()
                if last_report_time_diff <= 30:
                    continue
            self.logger.debug(
                "timeline_item in gas tank detect raw data:{}".format(item.raw_data))
            self.state_obj = {"last_infer_timestamp": datetime.datetime.now()}

            sections = item.raw_data.split('|')
            edge_board_confidence = sections[len(sections) - 1]
            event_alarms.append(
                event_alarm.EventAlarm(self, item.original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                       "detected gas tank entering elevator with board confid: {}".format(
                                           edge_board_confidence)))

        return None


#
# 遮挡门告警	判断电梯发生遮挡门事件 1.电梯内有人 2.门处于打开状态 3.电梯静止
class BlockingDoorEventDetector(EventDetectorBase):
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
        for det in event_detectors:
            if det.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                det.subscribe_on_property_changed(self)
                break

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        self.logger.debug(
            "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
                self.timeline.board_id,
                self.__class__.__name__,
                src_detector.__class__.__name__,
                property_name,
                str(data)))
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
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data or
                         (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data))]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        door_state = None
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        if not door_state or door_state["new_state"] != "OPEN":
            return None
        door_open_time_diff = (datetime.datetime.now(
        ) - door_state["notify_time"]).total_seconds()
        # 如果收到的开门状态时间还很短，那么不作遮挡判断
        if abs(door_open_time_diff) < 30:
            return None
        person_timeline_items = [i for i in filtered_timeline_items if
                                 i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and
                                 "Person|#" in i.raw_data]
        # 电梯内没有人 不判断遮挡
        if not len(person_timeline_items) > 0:
            return None
        latest_person_item = person_timeline_items[-1]
        detect_person_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                   latest_person_item.original_timestamp).total_seconds()
        # 最近一次识别到人类已经有一段时间，那么可以认为电梯内没人
        if detect_person_time_diff > 3:
            return None

        last_state_object = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_object = self.state_obj["last_notify_timestamp"]
        if last_state_object:
            notify_time_diff = (datetime.datetime.now() -
                                last_state_object).total_seconds()
            if notify_time_diff < 120:
                return None

        speed_timeline_items = [i for i in filtered_timeline_items if
                                i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                                "speed" in i.raw_data]
        # 未能获取到电梯速度，无法判断电梯是否静止
        if not len(speed_timeline_items) > 2:
            return None

        new_state_object = None
        latest_speed_item = speed_timeline_items[-1]
        latest_speed_item_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                       latest_speed_item.original_timestamp).total_seconds()
        third_speed_item = speed_timeline_items[-3]
        third_speed_item_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                      third_speed_item.original_timestamp).total_seconds()
        if abs(latest_speed_item_time_diff) < 4 and latest_speed_item.raw_data["speed"] < 0.1 and \
                abs(third_speed_item_time_diff) < 6 and third_speed_item.raw_data["speed"] < 0.2:
            new_state_object = datetime.datetime.now()
        if new_state_object:
            self.logger.debug("遮挡门告警中，开门时长为{}s".format(door_open_time_diff))
            self.state_obj["last_notify_timestamp"] = new_state_object
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "电梯门被遮挡", "002")]

        return None


#
# 困人	判断电梯发生困人事件
class PeopleStuckEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.logger = logging.getLogger(__name__)
        self.state_obj = {}

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
        self.logger.debug(
            "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
                self.timeline.board_id,
                self.__class__.__name__,
                src_detector.__class__.__name__,
                property_name,
                str(data)))
        if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__:
            # handle event from DoorStateChangedEventDetector
            # 记录最近一次电梯门的状态，
            if src_detector.__class__.__name__ == DoorStateChangedEventDetector.__name__ and \
                    property_name == "door_state":
                self.state_obj["door_state"] = {"new_state": data["new_state"],
                                                "last_state": data["last_state"],
                                                "notify_time": datetime.datetime.now(datetime.timezone.utc)}

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and
                    ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and (
                            "Person|#" in i.raw_data in i.raw_data)) or
                     (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED))]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")

        door_state = None
        if self.state_obj and "door_state" in self.state_obj:
            door_state = self.state_obj["door_state"]
        if not door_state or door_state["new_state"] == "OPEN":
            return None
        # if door_state["last_state"] == "OPEN" and (
        #        datetime.datetime.now() - door_state["notify_time"]).total_seconds() < 20:
        #    return None
        # 如果门关上还未超出2分钟则不认为困人
        if door_state["new_state"] == "CLOSE" and (
                datetime.datetime.now(datetime.timezone.utc) - door_state["notify_time"]).total_seconds() < 90:
            return None
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
        # "Person|#"
        person_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in
                                          i.raw_data
                                          and (door_state["notify_time"] - i.original_timestamp).total_seconds() < 0]
        # 电梯内没人
        if len(person_filtered_timeline_items) < 6:
            return None
        # object_person = None
        object_person = person_filtered_timeline_items[0]
        if (datetime.datetime.now(datetime.timezone.utc) - object_person.original_timestamp).total_seconds() < 20:
            return None
        self.logger.debug("困人检测中共发现{}人，最早目标出现在:{}".format(len(person_filtered_timeline_items),
                                                                          object_person.original_timestamp_str))
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
        if (datetime.datetime.now(datetime.timezone.utc) - person_filtered_timeline_items[
            0].original_timestamp).total_seconds() < 90:
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
            if time_diff <= 90 and abs(item.raw_data["speed"]) > 0.3:
                is_quiescent = False
        if is_quiescent:
            new_state_obj = {"people_stuck": "stuck",
                             "last_report_timestamp": datetime.datetime.now()}
        # store it back, and it will be passed in at next call
        self.state_obj["last_notify_timestamp"] = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "{}发现有人困在电梯内".format(
                                           new_state_obj["last_report_timestamp"].strftime("%d/%m/%Y %H:%M:%S")),
                                       "001")]
        return None


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
        self.state_obj = {"last_notify_time_stamp": datetime.datetime.now()}
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP or
                         (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data))]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        if self.state_obj and "last_notify_time_stamp" in self.state_obj and \
                (datetime.datetime.now() - self.state_obj["last_notify_time_stamp"]).total_seconds() < 300:
            return None
        last_state_obj = None if not (
                self.state_obj and "state" in self.state_obj) else self.state_obj["state"]
        new_state_obj = None
        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_speed_state": "unknown", "last_speed": 0,
                                 "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "speed" in item.raw_data and "maxVelocity" in item.raw_data:
                state = "normal"
                if abs(item.raw_data["speed"]) > item.raw_data["maxVelocity"]:
                    state = "overspeed"
                new_state_obj = {"last_speed_state": state, "last_speed": item.raw_data["speed"],
                                 "last_state_timestamp": str(datetime.datetime.now())}
                break
        if new_state_obj:
            new_state_obj = {"last_speed_state": "unknown", "last_speed": 0,
                             "last_state_timestamp": str(datetime.datetime.now())}
        # store it back, and it will be passed in at next call
        self.state_obj["state"] = new_state_obj

        if (last_state_obj and last_state_obj["last_speed_state"] == "overspeed" and
                new_state_obj and new_state_obj["last_speed_state"] == "overspeed"):
            self.state_obj["last_notify_time_stamp"] = datetime.datetime.now()
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯超速，当前速度：{}m/s".format(new_state_obj["last_speed"]), "0020")]
        return None


#
# 反复开关门	判断电梯发生反复开关门故障
class DoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
        self.alarms_to_fire = []
        self.timeline = None
        self.logger = logging.getLogger(__name__)

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

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data)]

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

                    if len(last_state_changed_times) >= 6:
                        total_time_gap = (last_state_changed_times[-1] - last_state_changed_times[-3]
                                          + last_state_changed_times[-3] - last_state_changed_times[
                                              -5]).total_seconds()
                        self.logger.debug(
                            "total_time_gap:{}".format(total_time_gap))
                        if total_time_gap <= 20:
                            self.state_obj["last_state_changed_times"] = []
                            last_report_time = None if not ("last_report_time" in self.state_obj) else self.state_obj[
                                "last_report_time"]
                            if last_report_time:
                                report_time_diff = (
                                        datetime.datetime.now() - last_report_time).total_seconds()
                                if report_time_diff < 20:
                                    self.logger.debug(
                                        "last report time:{},time_diff:{}".format(last_report_time, report_time_diff))
                                    return
                            self.state_obj["last_report_time"] = datetime.datetime.now(
                            )
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
                            self.state_obj["last_state_changed_times"] = []
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

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # 电梯内没有人 不判断反复开关
        if not len(filtered_timeline_items) > 0:
            self.alarms_to_fire.clear()
            return None
        latest_person_item = filtered_timeline_items[-1]
        detect_person_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                   latest_person_item.original_timestamp).total_seconds()
        # 最近一次识别到人类已经有一段时间，那么可以认为电梯内没人
        if detect_person_time_diff > 3:
            self.alarms_to_fire.clear()
            return None
        if len(self.alarms_to_fire) > 0:
            alarm = self.alarms_to_fire[0]
            target_persons = [i for i in filtered_timeline_items if (alarm.original_utc_timestamp - i.original_timestamp
                                                                     ).total_seconds() > 5 and
                              (alarm.original_utc_timestamp - i.original_timestamp).total_seconds() < 10]
            if len(target_persons) == 0:
                self.alarms_to_fire.clear()
                return None
            self.logger.debug(
                "反复开关门判断中出现人：{},raw:{}".format(target_persons[-1].original_timestamp_str,
                                                          target_persons[-1].raw_data))

        alarms = self.alarms_to_fire
        self.alarms_to_fire = []
        return alarms


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
        objects = [i for i in filtered_timeline_items if i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
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
        last_state_obj = None
        new_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_obj = self.state_obj["last_notify_timestamp"]
        if last_state_obj:
            last_notify_time_diff = (
                    datetime.datetime.now() - last_state_obj).total_seconds()
            if last_notify_time_diff < 10:
                return None
        temp_speed = 0.0
        speedItems = [i for i in filtered_timeline_items if
                      i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        for speed_timeline_item in reversed(speedItems):
            temp_speed = abs(speed_timeline_item.raw_data["speed"])
            door_open_speed_time_diff = (speed_timeline_item.original_timestamp -
                                         door_state["time"]).total_seconds()
            # 获取到的速度值在开门之后
            if temp_speed > 1 and door_open_speed_time_diff > 0:
                new_state_obj = datetime.datetime.now()
            break
        # 将报警时间存入状态字典
        self.state_obj["last_notify_timestamp"] = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯门在运行中打开,当前速度为:{}m/s".format(temp_speed), "0022")]
        return None


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
        if self.last_door_open_state_time \
                and (datetime.datetime.now() - self.last_door_open_state_time).total_seconds() >= 30:
            alarms = [event_alarm.EventAlarm(
                self,
                datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                event_alarm.EventAlarmPriority.ERROR,
                "长时间处于开门状态 - 门开着超过一定时间的故障, 最近一次门状态转变为'开'的时间为: {}, 且持续时间 >= {}秒".format(
                    self.last_door_open_state_time.strftime(
                        "%d/%m/%Y %H:%M:%S"),
                    (datetime.datetime.now() - self.last_door_open_state_time).total_seconds()))]
            self.last_door_open_state_time = None
            return alarms
        return None


#
# 无人高频运行	轿厢内没人, 上上下下跑来跑去
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
        pass

    def on_property_changed_event_handler(self, src_detector: EventDetectorBase, property_name: str, data):
        self.logger.debug(
            "board: {}, {} is notified by event from: {} for property: {} with data: {}".format(
                self.timeline.board_id,
                self.__class__.__name__,
                src_detector.__class__.__name__,
                property_name,
                str(data)))
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
                           or (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                               and "storey" in i.raw_data))]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        door_state_object = None if not (self.state_obj and self.state_obj["door_state"]) \
            else self.state_obj["door_state"]
        if (not door_state_object) or door_state_object["new_state"] == "OPEN":
            return None
        door_close_time_diff = (datetime.datetime.now(
        ) - door_state_object["notify_time"]).total_seconds()
        # 如果门处于关闭状态小于5分钟那么对于电梯要完成无人反复上下可能性不大
        if door_close_time_diff < 300:
            return None
        last_state_object = None if not (self.state_obj and self.state_obj["last_notify_timestamp"]) \
            else self.state_obj["last_notify_timestamp"]

        if last_state_object:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object).total_seconds()
            if last_report_time_diff < 120:
                return None
        person_timeline_item = [i for i in filtered_timeline_items if i.item_type ==
                                board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data]
        if len(person_timeline_item) > 0:
            latest_person_object = person_timeline_item[-1]
            latest_detect_person_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                              latest_person_object.original_timestamp).total_seconds()
            if latest_detect_person_time_diff < 600:
                return None
        storey_timeline_item = [i for i in filtered_timeline_items if i.item_type ==
                                board_timeline.TimelineItemType.SENSOR_READ_PRESSURE and "storey" in i.raw_data]
        # 如果楼层信息很少，那么没办法判断是否有往返
        if len(storey_timeline_item) < 50:
            return None
        storey_one_timeline_item = [
            i for i in storey_timeline_item if i.raw_data["storey"] == 1]
        storey_three_timeline_item = [
            i for i in storey_timeline_item if i.raw_data["storey"] == 10]
        if len(storey_one_timeline_item) > 3 and len(storey_three_timeline_item) > 3:
            if not self.state_obj:
                self.state_obj = {}
            self.state_obj["last_notify_timestamp"] = datetime.datetime.now()
            return [event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                           event_alarm.EventAlarmPriority.WARNING,
                                           "电梯无人，上下往返: {}次".format(len(storey_three_timeline_item)), "0019")]

        return None


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
                                       "电梯温度异常，当前温度: {}".format(new_state_obj["last_temperature_state"]),
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
                      and i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                      and "Person|#" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 30:
                return None
        # 识别到的人的记录小于10条，那么不算拥挤
        if not len(filtered_timeline_items) > 10:
            return None
        latest_person_object = filtered_timeline_items[-1]

        target_persons = [i for i in filtered_timeline_items if i.board_msg_id == latest_person_object.board_msg_id
                          and i.original_timestamp_str == latest_person_object.original_timestamp_str]
        self.logger.debug("当前人数{}".format(len(target_persons)))
        # store it back, and it will be passed in at next call
        if len(target_persons) > 5:
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
                      # and (datetime.datetime.now() - i.local_timestamp).total_seconds() <= 10
                      # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                      and ((i.item_type == board_timeline.TimelineItemType.SENSOR_READ_PRESSURE
                            and "storey" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED
                                   and "speed" in i.raw_data) or (
                                   i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                                   and "Person|#" in i.raw_data
                           ))]
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
            hasPerson = "Y" if len(person_timeline_items) > 0 else "N"
            code = ""
            speed = 0.00
            # LIFTUP LIFTDOWN
            if len(speed_timeline_items) > 4:
                last_speed_object = speed_timeline_items[-1]
                previous_speed_oject = speed_timeline_items[-2]
                speed = last_speed_object.raw_data["speed"]
                if abs(last_speed_object.raw_data["speed"]) < 0.1 and \
                        abs(previous_speed_oject.raw_data["speed"]) < 0.1:
                    code = "LIFTSTOP"
                # 如果电梯在运动，比较楼层的变化判断向上、向下
                elif not (abs(last_speed_object.raw_data["speed"]) < 0.1):
                    storey_latest = storey_timeline_items[-1].raw_data["storey"]
                    storey_last_fifth = storey_timeline_items[-5].raw_data["storey"]
                    code = "LIFTDOWN" if storey_latest < storey_last_fifth else "LIFTUP"

            storey = 0
            if len(storey_timeline_items) > 0:
                storey = storey_timeline_items[-1].raw_data["storey"]
            if last_state_object and "code" in last_state_object:
                if last_state_object["code"] == code and last_state_object["floor"] == storey and \
                        last_state_object["hasPerson"] == hasPerson:
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
                data = {"human_inside": hasPerson, "floor_num": str(storey)} if code == "LIFTSTOP" \
                    else {"human_inside": hasPerson, "floor_num": str(storey), "speed": str(speed)}
                alarms.append(event_alarm.EventAlarm(
                    self,
                    datetime.datetime.fromisoformat(
                        datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                    event_alarm.EventAlarmPriority.INFO,
                    "human_inside:{},floor_num:{},speed:{}".format(
                        hasPerson, storey, speed),
                    code, data))
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
        target_timeline_items = filtered_timeline_items[-5:]
        # 如果读取到的加速度值一直是零/相同
        latest_raw_acceleration = target_timeline_items[-1].raw_data["raw_acceleration"]
        result_timeline_items = [i for i in target_timeline_items if i.raw_data["raw_acceleration"] ==
                                 latest_raw_acceleration]
        if len(result_timeline_items) == 5:
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
# 陀螺仪故障 1，获取不到温度值/气压值 2，连续5次读到的气压值相同（正常情况下，即使不动每次读取的值也不一样）
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
        target_timeline_items = filtered_timeline_items[-5:]
        # 如果读取到的气压值一直是零/相同
        latest_raw_pressure = target_timeline_items[-1].raw_data["pressure"]
        result_timeline_items = [i for i in target_timeline_items if i.raw_data["pressure"] ==
                                 latest_raw_pressure]
        if len(result_timeline_items) == 5:
            msg = "获取不到气压值" if latest_raw_pressure == 0.000 else "连续五次获取到的气压值相同"
            new_state_object = {"code": "TLYJG", "message": msg,
                                "time_stamp": datetime.datetime.now()}
        result_timeline_items = [
            i for i in target_timeline_items if i.raw_data["temperature"] == 0.00]
        if len(result_timeline_items) == 5:
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
        if time_diff < 600:
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
        if not len(filtered_timeline_items) > 3:
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
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_object["time_stamp"]).total_seconds()
            # 检测顶部有人5分钟报一次
            if last_report_time_diff < 300:
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
        return alarms
