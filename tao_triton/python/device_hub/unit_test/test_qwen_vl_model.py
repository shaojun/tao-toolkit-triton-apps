import base64
import time
import unittest
import sys
import os
import logging
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer
from openai import OpenAI

class TestQWenVlModel(unittest.TestCase):

    def test_inference_video_images_from_ali_qwen_models_test_method_0(self):
        inferencer = Inferencer(logging.getLogger("dummy"))
        full_path_list = ["/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/1.png",
                          "/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/2.png",
                          "/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/3.png",
                          "/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/4.png"]
        # convert image to base64 encoded string
        image_file_base64_encoded_string_list  = []
        for full_path in full_path_list:
            with open(full_path, "rb") as image_file:
                image_file_base64_encoded_string_list.append(
                    base64.b64encode(image_file.read()).decode("utf-8"))
        result = inferencer.inference_video_images_from_ali_qwen_vl_plus_models(
            image_file_base64_encoded_string_list)
        print(result)
