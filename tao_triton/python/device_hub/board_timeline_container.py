import _thread
import threading
import time
from tao_triton.python.device_hub import board_timeline
import board_timeline
import event_detector
import logging
import logging.config
from typing import List


class BoardTimeLineContainer:
    def __init__(self, board_prefix: str, logging, kafka_producer, shared_EventAlarmWebServiceNotifier):
        self.container_key = board_prefix
        self.board_event_datas = []
        self.shared_kafka_producer = kafka_producer
        self.shared_EventAlarmWebServiceNotifier = shared_EventAlarmWebServiceNotifier
        self.logging = logging
        self.BOARD_TIMELINES = self.create_boardtimeline_from_web_service()
        _thread.start_new_thread(self.processEventDatas, ())

    def create_boardtimeline_from_web_service(self) -> List[board_timeline.BoardTimeline]:
        return []

    def add_event_data(self, data):
        self.board_event_datas.append(data)

    def processEventDatas(self):
        logger = logging.getLogger('__main__')
        while True:
            try:
                event_data = None
                if self.board_event_datas and len(self.board_event_datas) > 0:
                    event_data = self.board_event_datas.pop(0)
                if not event_data:
                    time.sleep(1)
                    continue

                board_msg_id = event_data["id"]
                # UTC datetime str, like: '2023-06-28T12:58:58.960Z'
                board_msg_original_timestamp = event_data["@timestamp"]
                board_id = event_data["sensorId"]
                cur_board_timeline = [t for t in self.BOARD_TIMELINES if
                                      t.board_id == board_id]
                if not cur_board_timeline:
                    cur_board_timeline = self.create_boardtimeline(board_id)
                    self.BOARD_TIMELINES.append(cur_board_timeline)
                else:
                    cur_board_timeline = cur_board_timeline[0]
                # indicates it's the object detection msg
                if "objects" in event_data:
                    new_timeline_items = []
                    if len(event_data["objects"]) == 0:
                        new_timeline_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.OBJECT_DETECT,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, ""))
                    for obj_data in event_data["objects"]:
                        new_timeline_items.append(
                            board_timeline.TimelineItem(cur_board_timeline,
                                                        board_timeline.TimelineItemType.OBJECT_DETECT,
                                                        board_msg_original_timestamp,
                                                        board_msg_id, obj_data))
                    cur_board_timeline.add_items(new_timeline_items)

                # indicates it's the sensor data reading msg
                elif "sensors" in event_data and "sensorId" in event_data:
                    new_items = []
                    for obj_data in event_data["sensors"]:
                        if "speed" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_SPEED,
                                                            board_msg_original_timestamp,
                                                            board_msg_id, obj_data))
                            # cur_board_timeline.add_items([new_timeline_item])
                        elif "pressure" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_PRESSURE,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                            # cur_board_timeline.add_items([new_timeline_item])
                        elif "ACCELERATOR" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_ACCELERATOR,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                            # cur_board_timeline.add_items([new_timeline_item])
                        elif "switchFault" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_ELECTRIC_SWITCH,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                        elif "detectPerson" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.SENSOR_READ_PEOPLE_DETECT,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                        elif "cameraBlocked" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.CAMERA_BLOCKED,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                        elif "eventType" in obj_data:
                            new_items.append(
                                board_timeline.TimelineItem(cur_board_timeline,
                                                            board_timeline.TimelineItemType.CAMERA_VECHILE_EVENT,
                                                            board_msg_original_timestamp, board_msg_id, obj_data))
                    cur_board_timeline.add_items(new_items)
                elif "update" in event_data:
                    new_update_timeline_items = [board_timeline.TimelineItem(cur_board_timeline,
                                                                             board_timeline.TimelineItemType.UPDATE_RESULT,
                                                                             board_msg_original_timestamp, board_msg_id,
                                                                             event_data)]
                    cur_board_timeline.add_items(new_update_timeline_items)
            except Exception as e:
                logger.exception("Major error caused by exception:")
                print(e)
                continue

    def create_boardtimeline(self, board_id: str):
        # these detectors instances are shared by all timelines
        event_detectors = [event_detector.ElectricBicycleEnteringEventDetector(self.logging),
                           event_detector.DoorStateChangedEventDetector(self.logging),
                           event_detector.BlockingDoorEventDetector(self.logging),
                           event_detector.PeopleStuckEventDetector(self.logging),
                           event_detector.GasTankEnteringEventDetector(self.logging),
                           event_detector.DoorOpenedForLongtimeEventDetector(self.logging),
                           event_detector.DoorRepeatlyOpenAndCloseEventDetector(self.logging),
                           event_detector.ElevatorOverspeedEventDetector(self.logging),
                           # event_detector.TemperatureTooHighEventDetector(logging),
                           # event_detector.PassagerVigorousExerciseEventDetector(logging),
                           # event_detector.DoorOpeningAtMovingEventDetector(logging),
                           # event_detector.ElevatorSuddenlyStoppedEventDetector(logging),
                           # event_detector.ElevatorShockEventDetector(logging),
                           event_detector.ElevatorMovingWithoutPeopleInEventDetector(self.logging),
                           event_detector.ElevatorJamsEventDetector(self.logging),
                           event_detector.ElevatorMileageEventDetector(self.logging),
                           event_detector.ElevatorRunningStateEventDetector(self.logging),
                           event_detector.UpdateResultEventDetector(self.logging),
                           event_detector.GyroscopeFaultEventDetector(self.logging),
                           event_detector.PressureFaultEventDetector(self.logging),
                           event_detector.ElectricSwitchFaultEventDetector(self.logging),
                           event_detector.DeviceOfflineEventDetector(self.logging),
                           event_detector.DetectPersonOnTopEventDetector(self.logging),
                           event_detector.DetectCameraBlockedEventDetector(self.logging),
                           # event_detector.CameraDetectVehicleEventDetector(logging)
                           ]
        return board_timeline.BoardTimeline(self.logging, board_id, [],
                                            event_detectors,
                                            [  # event_alarm.EventAlarmDummyNotifier(logging),
                                                self.shared_EventAlarmWebServiceNotifier],
                                            self.shared_kafka_producer)