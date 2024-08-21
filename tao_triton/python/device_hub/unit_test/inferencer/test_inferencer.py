
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer
import time
import unittest
import sys
import os
from tao_triton.python.device_hub.utility.session_window import *


class TestInferencer(unittest.TestCase):

    def test_gastank(self):
        inferencer = Inferencer(None)
        # get current running path
        current_path = os.path.abspath(os.path.dirname(__file__))
        gastank_image_folder_path = os.path.join(
            current_path, "assets", "gastank")
        gastank_image_file_list = os.listdir(gastank_image_folder_path)
        for image_file in gastank_image_file_list:
            image_file_path = os.path.join(
                gastank_image_folder_path, image_file)
            infered_class, infered_confid = inferencer.inference_image_file_from_qua_models(
                image_file_path)
            print(
                f"{image_file} - infered_class: {infered_class}, infered_confid: {infered_confid}")

    def test_start_inference_image_file_from_qua_models(self):
        inferencer = Inferencer(None)
        # get current running path
        current_path = os.path.abspath(os.path.dirname(__file__))
        gastank_image_folder_path = os.path.join(
            current_path, "assets", "gastank")
        gastank_image_file_list = os.listdir(gastank_image_folder_path)

        def callback(infered_result):
            infered_class, infered_confid = infered_result
            print(
                f"callback - infered_class: {infered_class}, infered_confid: {infered_confid}")
        for image_file in gastank_image_file_list:
            image_file_path = os.path.join(
                gastank_image_folder_path, image_file)
            inferencer.start_inference_image_file_from_qua_models(
                image_file_path, callback)
