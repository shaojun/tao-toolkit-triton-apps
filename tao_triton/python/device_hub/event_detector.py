import datetime
import os
import re
from typing import List

# from tao_triton.python.device_hub import base64_tao_client
# from tao_triton.python.device_hub.board_timeline import TimelineItem, TimelineItemType
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
        self.logger.debug(
            "{} is notified by event from: {} for property: {} with data: {}".format(
                self.__class__.__name__,
                src_detector.__class__.__name__,
                property_name,
                str(data)))
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
                        (datetime.datetime.now() - i.local_timestamp).total_seconds() <= 3]
        for ri in reversed(recent_items):
            if ri.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_door_state": "OPEN", "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "Vehicle|#|DoorWarningSign" in ri.raw_data:
                new_state_obj = {"last_door_state": "CLOSE", "last_state_timestamp": str(datetime.datetime.now())}
                break

        if not new_state_obj:
            new_state_obj = {"last_door_state": "OPEN", "last_state_timestamp": str(datetime.datetime.now())}

        # store it back
        self.state_obj = new_state_obj
        if (last_state_obj and last_state_obj["last_door_state"] != new_state_obj[
            "last_door_state"]) or last_state_obj is None:
            self.__fire_on_property_changed_event_to_subscribers__(
                "door_state",
                {"last_state": "None" if last_state_obj is None else last_state_obj["last_door_state"],
                 "new_state": new_state_obj["last_door_state"]})
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.INFO,
                                       "Door state changed to: {}".format(new_state_obj["last_door_state"]))]
        return None


#
# 电动车检测告警（电动车入梯)
class ElectricBicycleEnteringEventDetector(EventDetectorBase):
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
                      and "Vehicle|#|TwoWheeler" in i.raw_data]
            return result

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        last_state_obj = self.state_obj
        new_state_obj = None

        event_alarms = []
        for item in filtered_timeline_items:
            item.consumed = True
            if last_state_obj and "last_infer_ebic_timestamp" in last_state_obj:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - last_state_obj["last_infer_ebic_timestamp"]).total_seconds()
                if last_report_time_diff <= 10:
                    continue

            new_state_obj = {"last_infer_ebic_timestamp": datetime.datetime.now()}

            sections = item.raw_data.split('|')
            self.logger.debug(
                "board: {}, E-bic is detected and re-infer it from triton...".format(item.timeline.board_id))
            self.__fire_on_property_changed_event_to_subscribers__("E-bic Entering",
                                                                   {"detail": "there's a EB incoming"})
            # the last but one is the detected object image file with base64 encoded text,
            # and the section is prefixed with-> base64_image_data:
            cropped_base64_image_file_text = sections[len(sections) - 2][len("base64_image_data:"):]
            edge_board_confidence = sections[len(sections) - 1]
            # infer_results = base64_tao_client.infer(FLAGS.verbose, FLAGS.async_set, FLAGS.streaming,
            #                                         FLAGS.model_name, FLAGS.model_version,
            #                                         FLAGS.batch_size, FLAGS.class_list,
            #                                         False, FLAGS.url, FLAGS.protocol, FLAGS.mode,
            #                                         FLAGS.output_path,
            #                                         [cropped_base64_image_file_text])
            infer_results = base64_tao_client.infer(False, False, False,
                                                    "bicycletypenet_tao", "",
                                                    1, "bicycle,electric_bicycle",
                                                    False, "36.153.41.18:18000", "HTTP", "Classification",
                                                    os.path.join(os.getcwd(), "outputs"),
                                                    [cropped_base64_image_file_text])
            # sample: (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(0)=bicycle, 0.4476(1)=electric_bicycle
            # the `0.4476(1)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
            # which is less than 50%, so it's a bicycle, should not trigger alarm.
            self.logger.debug("      (localConf:{})infer_results: {}".format(edge_board_confidence, infer_results))
            m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)', infer_results)
            if m and m.group(0):
                infer_server_confid = float(m.group(0))
                if infer_server_confid >= 0.5:
                    event_alarms.append(
                        event_alarm.EventAlarm(self, item.original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                               "detected electric-bicycle entering elevator with board confid: {}, server confid: {}".format(
                                                   edge_board_confidence, infer_server_confid)))
                else:
                    self.logger.debug(
                        "      sink this detect due to infer server give low confidence: {}".format(m.group(0)))

        return event_alarms


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
                    property_name == "door_state" and data["new_state"] == "OPEN":
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
        door_open_time_diff = (datetime.datetime.now() - door_state["notify_time"]).total_seconds()
        # 如果收到的开门状态时间还很短，那么不作遮挡判断
        if abs(door_open_time_diff) < 5:
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
        if detect_person_time_diff > 4:
            return None

        last_state_object = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_object = self.state_obj["last_notify_timestamp"]
        if last_state_object:
            notify_time_diff = (datetime.datetime.now() - last_state_object).total_seconds()
            if notify_time_diff < 5:
                return None

        speed_timeline_items = [i for i in filtered_timeline_items if
                                i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and
                                "speed" in i.raw_data]
        # 未能获取到电梯速度，无法判断电梯是否静止
        if not len(speed_timeline_items) > 0:
            return None

        new_state_object = None
        latest_speed_item = speed_timeline_items[-1]
        latest_speed_item_time_diff = (datetime.datetime.now(datetime.timezone.utc) -
                                       latest_speed_item.original_timestamp).total_seconds()
        if abs(latest_speed_item_time_diff) < 4 and latest_speed_item.raw_data["speed"] < 0.1:
            new_state_object = datetime.datetime.now()
        if new_state_object:
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
            return [i for i in timeline_items if
                    not i.consumed
                    and
                    ((i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and ("Person|#" in i.raw_data or
                                                                                        "Vehicle|#|DoorWarningSign" in i.raw_data)) or
                     (i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED))]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
        last_state_obj = self.state_obj
        new_state_obj = None
        if last_state_obj and "last_report_timestamp":
            last_report_time_diff = (
                    datetime.datetime.now() - last_state_obj["last_report_timestamp"]).total_seconds()
            # 如果短时间内上报过
            if last_report_time_diff <= 5:
                return None
        # "Person|#"
        person_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT and "Person|#" in i.raw_data]
        # 电梯内没人
        if person_filtered_timeline_items and len(person_filtered_timeline_items) <= 0:
            return None
        object_person = None
        for person in reversed(person_filtered_timeline_items):
            latest_time_diff = (
                    datetime.datetime.now(datetime.timezone.utc) - person.original_timestamp).total_seconds()
            if latest_time_diff < 5:
                object_person = person
            break
        # 如果在5秒内没有发现有人，那么不认为有困人
        if not object_person:
            return None
        # Vehicle|#|DoorWarningSign
        door_filtered_timeline_items = [i for i in filtered_timeline_items if
                                        i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT
                                        and "Vehicle|#|DoorWarningSign" in i.raw_data]
        # 没有门标，则认为门是开着的
        if door_filtered_timeline_items and len(door_filtered_timeline_items) <= 0:
            return None
        # 如果5秒内都没有检测到门标则认为门是开着的
        object_door_sign = None
        for sign in reversed(door_filtered_timeline_items):
            latest_time_diff = (datetime.datetime.now() - sign.original_timestamp).total_seconds()
            if latest_time_diff < 5:
                object_door_sign = sign
            break
        if not object_door_sign:
            return None
        # speed
        speed_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                         board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        if speed_filtered_timeline_items and len(speed_filtered_timeline_items) <= 0:
            return None
        is_quiescent = True
        for item in speed_filtered_timeline_items:
            time_diff = (datetime.datetime.now(datetime.timezone.utc) - item.original_timestamp).total_seconds()
            # 如果90秒内电梯有在运动那么不认为困人
            if time_diff <= 90 and abs(item.raw_data["speed"]) > 0.1:
                is_quiescent = False
        if is_quiescent:
            new_state_obj = {"people_stuck": "stuck", "last_report_timestamp": str(datetime.datetime.now())}
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.ERROR,
                                       "有人困在电梯内", "001")]

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
        last_state_obj = self.state_obj
        new_state_obj = None
        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_speed_state": "unknown", "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "speed" in item.raw_data and "maxVelocity" in item.raw_data:
                state = "normal"
                if abs(item.raw_data["speed"]) > item.raw_data["maxVelocity"]:
                    state = "overspeed"
                new_state_obj = {"last_speed_state": state, "last_state_timestamp": str(datetime.datetime.now())}
                break
        if not new_state_obj:
            new_state_obj = {"last_speed_state": "unknown", "last_state_timestamp": str(datetime.datetime.now())}
        # store it back, and it will be passed in at next call
        self.state_obj = new_state_obj
        if (last_state_obj and last_state_obj["last_speed_state"] == "overspeed" and
                new_state_obj and new_state_obj["last_speed_state"] == "overspeed"):
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯超速: {}".format(new_state_obj["last_speed_state"]), "0020")]
        return None


#
# 反复开关门	判断电梯发生反复开关门故障
class DoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        EventDetectorBase.__init__(self, logging)
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
            if property_name == "door_state":
                # state values are: OPEN, CLOSE, None
                data["last_state"]
                data["new_state"]
                pass

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        return None


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
        if last_state_obj:
            last_report_time_diff = (
                    datetime.datetime.now(datetime.timezone.utc) - last_state_obj[
                "last_notify_timestamp"]).total_seconds()
            if last_report_time_diff < 10:
                return None

        object_filtered_timeline_items = [i for i in filtered_timeline_items if
                                          i.item_type == board_timeline.TimelineItemType.OBJECT_DETECT]
        acceleration_filtered_timeline_items = [i for i in filtered_timeline_items if i.item_type ==
                                                board_timeline.TimelineItemType.SENSOR_READ_SPEED]
        detect_person = None
        for item in reversed(object_filtered_timeline_items):
            if (datetime.datetime.now(datetime.timezone.utc) - item.original_timestamp).total_seconds() < 5:
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
                property_name == "door_state" and data["new_state"] == "OPEN":
            if self.state_obj and "door_state_open" in self.state_obj:
                self.state_obj["door_state_open"] = datetime.datetime.now()
            else:
                self.state_obj["door_state_open"] = datetime.datetime.now()

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            return [i for i in timeline_items if
                    not i.consumed
                    and i.item_type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "speed" in i.raw_data]

        return filter

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        door_state_open = None if not (self.state_obj and "door_state_open" in self.state_obj) \
            else self.state_obj["door_state_open"]
        # 如果没有开门事件，则不认为有在运动中开门的事发生
        if not door_state_open:
            return None

        last_state_obj = None
        new_state_obj = None
        if self.state_obj and "last_notify_timestamp" in self.state_obj:
            last_state_obj = self.state_obj["last_notify_timestamp"]
        if last_state_obj:
            last_notify_time_diff = (
                    datetime.datetime.now() - last_state_obj).total_seconds()
            if last_notify_time_diff < 5:
                return None
        for speed_timeline_item in reversed(filtered_timeline_items):
            temp_speed = abs(speed_timeline_item.raw_data["speed"])

            door_open_speed_time_diff = (datetime.datetime.now() -
                                         door_state_open).total_seconds()
            if temp_speed > 0.1 and door_open_speed_time_diff <= 2:
                new_state_obj = datetime.datetime.now()
            break
        # 将报警时间存入状态字典
        self.state_obj["last_notify_timestamp"] = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯门在运行中打开", "0022")]
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
            if last_notify_time_diff < 5:
                return None

        if len(filtered_timeline_items) < 2:
            return None

        latest_speed_item = filtered_timeline_items[-1]
        previous_speed_item = filtered_timeline_items[-2]

        speed_change_time_diff = (previous_speed_item.original_timestamp -
                                  latest_speed_item.original_timestamp).total_seconds()
        if abs(speed_change_time_diff) > 4:
            None
        if latest_speed_item and abs(latest_speed_item.raw_data["speed"]) < 0.1 \
                and previous_speed_item and abs(previous_speed_item.raw_data["speed"]) > 1:
            new_state_obj = {"current_speed": latest_speed_item.raw_data["speed"],
                             "previous_speed": previous_speed_item.raw_data["speed"],
                             "last_notify_timestamp": datetime.datetime.now()}
        self.state_obj = new_state_obj
        if new_state_obj:
            return [
                event_alarm.EventAlarm(self, datetime.datetime.fromisoformat(
                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                       event_alarm.EventAlarmPriority.WARNING,
                                       "电梯发生急停,速度从{}m/s变成{}".format(new_state_obj["previous_speed"],
                                                                    new_state_obj["current_speed"]), "006")]
        return None


#
# 长时间开门	门开着超过一定时间
class DoorOpenedForLongtimeEventDetector(EventDetectorBase):
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
        for ed in event_detectors:
            if ed.__class__.__name__ == DoorStateChangedEventDetector.__name__:
                ed.subscribe_on_property_changed(self)
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
            if property_name == "door_state":
                # state values are: OPEN, CLOSE, None
                data["last_state"]
                data["new_state"]
                pass

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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
                      and "temprature" in i.raw_data]
            return result
        return filter

    def detect(self, filtered_timeline_items):
        last_state_obj = self.state_obj
        new_state_obj = None
        for item in reversed(filtered_timeline_items):
            if item.item_type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_temperature_state": "normal", "last_notify_timestamp": datetime.datetime.now()}
                break
            elif "temprature" in item.raw_data:
                state = "normal"
                if abs(item.raw_data["temprature"]) > 10:
                    state = str(item.raw_data["temprature"])
                new_state_obj = {"last_temperature_state": state, "last_notify_timestamp": datetime.datetime.now()}
                break
        if not new_state_obj:
            new_state_obj = {"last_temperature_state": "unknown", "last_notify_timestamp": datetime.datetime.now()}
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
                                       "电梯温度异常，当前温度: {}".format(new_state_obj["last_temperature_state"]), "0013")]
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
