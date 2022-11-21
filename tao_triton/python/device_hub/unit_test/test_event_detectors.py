import re
from unittest import TestCase
# from tao_triton.python.device_hub.event_detector import *
from typing import List
from tao_triton.python.device_hub.board_timeline import BoardTimeline
import logging
from tao_triton.python.device_hub.event_detector import ElectricBicycleEnteringEventDetector
logging.getLogger(ElectricBicycleEnteringEventDetector.__name__).addHandler(
            logging.NullHandler())
class TestElectricBicycleEnteringEventDetector(TestCase):
    def test_resolve_infer_server_confidence(self):
        infer_result_sample = " (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(0)=bicycle, 0.4476(1)=electric_bicycle"
        # the `0.4476(1)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
        # which is less than 50%, so it's a bicycle, should not trigger alarm.
        m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)',
                      infer_result_sample)
        self.assertTrue(m.group(0) == '0.4476')

    def test0_resolve_elenet_four_classes_model_infer_server_confidence(self):
        infer_result_sample = "'temp_infer_image_files/0.jpg, 0.6880(3)=people, 0.2227(0)=background, 0.0709(2)=electric_bicycle, 0.0184(1)=bicycle'"
        m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)',
                      infer_result_sample)
        self.assertTrue(m)
        self.assertTrue(m.group(0) == '0.0709')

    def test1_resolve_elenet_four_classes_model_infer_server_confidence(self):
        import sys
        pa = sys.path
        print(sys.path)
        infer_result_sample = "'temp_infer_image_files/0.jpg, 0.6880(3)=people, 0.2227(0)=background, 0.0184(1)=bicycle'"
        m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)',
                      infer_result_sample)
        self.assertTrue(m == None)

    def test0_elenet_four_classes_model_infer_server_confidence(self):
        import datetime
        infer_result_sample = "temp_infer_image_files/0.jpg, 0.6880(3)=people, 0.2227(0)=background, 0.0184(1)=bicycle"

        detector = ElectricBicycleEnteringEventDetector(logging)
        board_timeline = self.create_boardtimeline(
            'aaabbbccc', logging, [detector])
        detector.prepare(board_timeline, None)
        generated_alarms = []
        generated_alarms = detector.__process_infer_result__(
            datetime.datetime.now(),
            0.88, '',
            infer_result_sample, False)
        self.assertFalse(generated_alarms)

    def test1_elenet_four_classes_model_infer_server_confidence(self):
        import datetime

        detector = ElectricBicycleEnteringEventDetector(logging)
        board_timeline = self.create_boardtimeline(
            'aaabbbccc', logging, [detector])
        detector.prepare(board_timeline, None)

        infer_result_sample = "temp_infer_image_files/0.jpg, 0.6880(3)=people, 0.2227(0)=background, 0.0709(2)=electric_bicycle, 0.0184(1)=bicycle"
        generated_alarms = []
        generated_alarms = detector.__process_infer_result__(
            datetime.datetime.now(),
            0.88, 'abc',
            infer_result_sample, False)
        self.assertFalse(generated_alarms)

    def create_boardtimeline(self, board_id: str, logging, event_detectors) -> List[BoardTimeline]:
        import logging
        from typing import List
        return BoardTimeline(logging, board_id, [],
                             event_detectors,
                             [
            # event_alarm.EventAlarmDummyNotifier(logging),
            # event_alarm.EventAlarmWebServiceNotifier(logging)
        ])
