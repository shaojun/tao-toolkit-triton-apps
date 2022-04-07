import re
from unittest import TestCase


class TestElectricBicycleEnteringEventDetector(TestCase):
    def test_resolve_infer_server_confidence(self):
        infer_result_sample = " (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(0)=bicycle, 0.4476(1)=electric_bicycle"
        # the `0.4476(1)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
        # which is less than 50%, so it's a bicycle, should not trigger alarm.
        m = re.search('\d\.\d+(?=\(\d\)\=electric_bicycle)', infer_result_sample)
        self.assertTrue(m.group(0) == '0.4476')
