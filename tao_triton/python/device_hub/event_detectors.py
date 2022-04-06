from typing import List

from python.device_hub.board_timeline import *


#
# 门的基本状态改变检测： 已开门， 已关门
class DoorStateChangedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    # def get_timeline_item_filter(self):
    #     def filter(timeline_items: List[TimelineItem]):
    #         return [i for i in timeline_items if
    #                 i.type == TimelineItemType.OBJECT_DETECT and
    #                 not i.consumed and "Vehicle|#|DoorWarningSign" in i.raw_data]
    #
    #     return filter

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        last_state_obj = None
        new_state_obj = None
        if state_obj_wrapper and state_obj_wrapper[0]:
            last_state_obj = state_obj_wrapper[0]
            last_door_state = last_state_obj["last_door_state"]
            last_state_timestamp = last_state_obj["last_state_timestamp"]
            if "extra_data" in last_state_obj:
                extra_data = last_state_obj["extra_data"]
        recent_items = [i for i in filtered_timeline_items if
                        (datetime.datetime.now() - i.local_timestamp).total_seconds() <= 4]
        for ri in reversed(recent_items):
            if ri.type == TimelineItemType.LOCAL_IDLE_LOOP:
                new_state_obj = {"last_door_state": "OPEN", "last_state_timestamp": str(datetime.datetime.now())}
                break
            elif "Vehicle|#|DoorWarningSign" in ri.raw_data:
                new_state_obj = {"last_door_state": "CLOSE", "last_state_timestamp": str(datetime.datetime.now())}
                break

        if not new_state_obj:
            new_state_obj = {"last_door_state": "OPEN", "last_state_timestamp": str(datetime.datetime.now())}

        # store it back, and it will be passed in at next call
        state_obj_wrapper[0] = new_state_obj
        if (last_state_obj and last_state_obj["last_door_state"] != new_state_obj[
            "last_door_state"]) or last_state_obj == None:
            return [
                EventAlarm(EventAlarmPriority.INFO,
                           "Door state changed to: {}".format(new_state_obj["last_door_state"]))]
        return None


#
# 电动车检测告警（电动车入梯)
class ElectricBicycleEnteringEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    not i.consumed
                    and
                    # (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                    (i.type == TimelineItemType.OBJECT_DETECT and "Vehicle|#|TwoWheeler" in i.raw_data)]

        return filter

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        last_state_obj = None
        new_state_obj = None
        if state_obj_wrapper and state_obj_wrapper[0]:
            last_state_obj = state_obj_wrapper[0]

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
            # store it back, and it will be passed in at next call
            state_obj_wrapper[0] = new_state_obj

            sections = item.raw_data.split('|')
            self.logger.info(
                "E-bic is detected at board: {}, re-infer it from triton...".format(item.board_id))
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
            self.logger.info("      (localConf:{})infer_results: {}".format(edge_board_confidence, infer_results))

            event_alarms.append(
                EventAlarm(EventAlarmPriority.ERROR, "detected a case of electric-bicycle entering elevator"))

            # there're may have several electric_bicycle detected in single msg, for lower the cost of infer, here only detecting the first one.

        return event_alarms


#
# 遮挡门告警	判断电梯发生遮挡门事件
class BlockingDoorEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 困人	判断电梯发生困人事件
class PeopleStuckEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        # "Person|#"
        # return EventAlarm(EventAlarmPriority.Error, "detected PeopleStuckEvent")
        return None


#
# 超速	判断电梯发生超速故障
class ElevatorOverspeedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def get_timeline_item_filter(self):
        def filter(timeline_items: List[TimelineItem]):
            return [i for i in timeline_items if
                    not i.consumed
                    and (i.type == TimelineItemType.LOCAL_IDLE_LOOP or
                         (i.type == TimelineItemType.SENSOR_READ_SPEED and "xxxxxx" in i.raw_data))]

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 反复开关门	判断电梯发生反复开关门故障
class ElevatorDoorRepeatlyOpenAndCloseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 剧烈运动	判断轿厢乘客剧烈运动
class PassagerVigorousExerciseEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 运行中开门	判断电梯发生运行中开门故障
class DoorOpeningAtMovingEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 急停	判断电梯发生急停故障
class ElevatorSuddenlyStoppedEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 长时间开门	门开着超过一定时间
class DoorOpenedForLongtimeEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None


#
# 无人高频运行	轿厢内没人, 上上下下跑来跑去
class ElevatorMovingWithoutPeopleInEventDetector(EventDetectorBase):
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def detect(self, state_obj_wrapper: [], filtered_timeline_items: List[TimelineItem]) -> List[EventAlarm]:
        return None
