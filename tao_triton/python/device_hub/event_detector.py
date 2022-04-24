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

        event_alarms = []
        for item in filtered_timeline_items:
            item.consumed = True
            if self.state_obj and "last_infer_ebic_timestamp" in self.state_obj:
                # we don't want to report too freq
                last_report_time_diff = (
                        datetime.datetime.now() - self.state_obj["last_infer_ebic_timestamp"]).total_seconds()
                if last_report_time_diff <= 15:
                    continue

            self.state_obj = {"last_infer_ebic_timestamp": datetime.datetime.now()}

            sections = item.raw_data.split('|')
            # self.logger.debug(
            #     "board: {}, E-bic is detected and re-infer it from triton...".format(item.timeline.board_id))

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
            self.logger.debug("      board: {}, (localConf:{})infer_results: {}".format(
                self.timeline.board_id,
                edge_board_confidence, infer_results))
            m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)', infer_results)
            if m and m.group(0):
                infer_server_confid = float(m.group(0))
                if infer_server_confid >= 0.5:
                    self.__fire_on_property_changed_event_to_subscribers__("E-bic Entering",
                                                                           {"detail": "there's a EB incoming"})
                    event_alarms.append(
                        event_alarm.EventAlarm(self, item.original_timestamp, event_alarm.EventAlarmPriority.ERROR,
                                               "detected electric-bicycle entering elevator with board confid: {}, server confid: {}".format(
                                                   edge_board_confidence, infer_server_confid)))
                else:
                    self.logger.debug(
                        "      board: {}, sink this detect due to infer server give low confidence: {}".format(
                            self.timeline.board_id,
                            m.group(0)))

        return event_alarms


#
# 遮挡门告警	判断电梯发生遮挡门事件
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
        pass

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
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
                    and (i.type == board_timeline.TimelineItemType.LOCAL_IDLE_LOOP or
                         (i.type == board_timeline.TimelineItemType.SENSOR_READ_SPEED and "xxxxxx" in i.raw_data))]

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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
                if self.state_obj and "last_state_changed_times" in self.state_obj:
                    self.state_obj["last_state_changed_times"].append(datetime.datetime.now())
                    last_state_changed_times: List[datetime] = self.state_obj["last_state_changed_times"]
                    if len(last_state_changed_times) >= 3:
                        total_time_gap = (last_state_changed_times[-1] - last_state_changed_times[-2]
                                          + last_state_changed_times[-2] - last_state_changed_times[
                                              -3]).total_seconds()
                        if total_time_gap <= 15:
                            self.state_obj["last_state_changed_times"] = []
                            self.alarms_to_fire.append(event_alarm.EventAlarm(
                                self,
                                datetime.datetime.fromisoformat(
                                    datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()),
                                event_alarm.EventAlarmPriority.ERROR,
                                "反复开关门 - 判断电梯发生反复开关门故障,3次开关门的总间隔<=15秒,最近一、二、三次 开/关门 时间分别为: {}  {}  {}".format(
                                    last_state_changed_times[-1].strftime("%d/%m/%Y %H:%M:%S"),
                                    last_state_changed_times[-2].strftime("%H:%M:%S"),
                                    last_state_changed_times[-3].strftime("%H:%M:%S"))))

                        self.state_obj["last_state_changed_times"] = self.state_obj["last_state_changed_times"][-3:-1]

                else:
                    self.state_obj = {"last_state_changed_times": [datetime.datetime.now()]}
                # door state values are: OPEN, CLOSE, None
                # data["last_state"]
                # data["new_state"]
                # pass

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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
        pass

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
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
