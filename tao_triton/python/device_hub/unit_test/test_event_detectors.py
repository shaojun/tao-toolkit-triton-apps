import re
import os
import sys
import datetime
from unittest import TestCase
# from tao_triton.python.device_hub.event_detector import *
from typing import List
# from tao_triton.python.device_hub.board_timeline import BoardTimeline
import logging
# from tao_triton.python.device_hub.event_detector import ElectricBicycleEnteringEventDetector
# from tao_triton.python.device_hub import event_alarm
# logging.getLogger(ElectricBicycleEnteringEventDetector.__name__).addHandler(
#             logging.NullHandler())


class test_ElectricBicycleEnteringEventDetector(TestCase):
    def __init__(self, *args, **kwargs):
        super(test_ElectricBicycleEnteringEventDetector,
              self).__init__(*args, **kwargs)

    def test_check_throttle_needed(self):
        path = sys.path
        sys.path.append(os.path.join(
            sys.path[1], 'tao_triton/python/device_hub'))
        from tao_triton.python.device_hub import event_alarm
        from tao_triton.python.device_hub.event_detector import ElectricBicycleEnteringEventDetector

        logging.getLogger(ElectricBicycleEnteringEventDetector.__name__).addHandler(
            logging.NullHandler())
        ElectricBicycleEnteringEventDetector.THROTTLE_Window_Depth = 2
        instance = ElectricBicycleEnteringEventDetector(logging)
        for i in range(0, 2, 1):
            t0 = datetime.datetime.now()
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == True)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            t1 = datetime.datetime.now()
            print("time cost: {}".format(t1-t0))
            import time
            time.sleep(
                ElectricBicycleEnteringEventDetector.THROTTLE_Window_Time_By_Sec+1)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == True)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            time.sleep(
                ElectricBicycleEnteringEventDetector.THROTTLE_Window_Time_By_Sec/3)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
            self.assertTrue(actual == False)
            time.sleep(
                ElectricBicycleEnteringEventDetector.THROTTLE_Window_Time_By_Sec+1)
        ElectricBicycleEnteringEventDetector.THROTTLE_Window_Depth = 1
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()
        self.assertTrue(actual == False)
        actual = instance.notifyEbIncomingAndCheckIfThrottleNeeded()

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
