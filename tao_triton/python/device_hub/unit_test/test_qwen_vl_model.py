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
        image_file_base64_encoded_string_list = []
        for full_path in full_path_list:
            with open(full_path, "rb") as image_file:
                image_file_base64_encoded_string_list.append(
                    base64.b64encode(image_file.read()).decode("utf-8"))
        result = inferencer.inference_video_images_from_ali_qwen_vl_plus_models(
            image_file_base64_encoded_string_list)
        print(result)

    def test_inference_video_images_from_ali_qwen_models_test_method_1(self):
        inferencer = Inferencer(logging.getLogger("dummy"))
        # convert image to base64 encoded string
        image_file_base64_encoded_string_list = ["data:image,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"]
        result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
            image_file_base64_encoded_string_list, model_name="qwen-vl-max-0809",
            user_prompt="请回答",
            system_prompt="你是一个数字识别专家,此图片是电梯中的楼层显示屏,里面显示的楼层数是多少?必须以json格式返回结果,例如: {'floor_number': 1999}")
        print(result)
