import base64
from concurrent.futures import ThreadPoolExecutor
import io
import json
import logging
import os
import time
from typing import Callable, Literal
import uuid
from PIL import Image
import requests


class Inferencer:
    executor = ThreadPoolExecutor(max_workers=15)

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def inference_image_file_from_qua_models(self, file_full_path: str) -> tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]:
        # get base64 image data
        with open(file_full_path, "rb") as image_file:
            base64_image_data_text = base64.b64encode(
                image_file.read()).decode('ascii')
            return self.inference_image_from_qua_models(base64_image_data_text)

    def start_inference_image_file_from_qua_models(self, file_full_path: str,
                                                   callback: Callable[[tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]], None]):
        # put the work into thread pool
        fut = Inferencer.executor.submit(
            self.inference_image_file_from_qua_models, file_full_path)
        fut.add_done_callback(lambda future: callback(future.result()))

    def inference_image_from_qua_models(self, base64_image_data_text: str) -> tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]:
        try:
            infer_start_time = time.time()
            infer_server_url = "http://36.139.163.39:18090/detect_images"
            data = {'input_image': base64_image_data_text,
                    'confidence_hold': 0.35}

            http_response = requests.post(
                infer_server_url, json=data)
            t1 = time.time()
            infer_used_time = (t1 - infer_start_time) * 1000
            infer_results = json.loads(http_response.text)
            # ['bicycle', 'ebicycle']
            infered_class_raw = infer_results['name']
            if infered_class_raw == 'ebicycle':
                infered_class = 'electric_bicycle'
            else:
                infered_class = infered_class_raw
            infered_confid: float = infer_results['confidence']
            # self.logger.debug(
            #     "      board: {}, time used for qua infer: {}ms(localConf: {}), raw infer_results/confid: {}/{}".format(
            #         self.timeline.board_id, str(infer_used_time)[:5],
            #         edge_board_confidence, infered_class, infered_server_confid))
            # self.statistics_logger.debug("{} | {} | {}".format(
            #     self.timeline.board_id,
            #     "1st_qua_model_post_infer",
            #     "infered_class: {}, infered_confid: {}, used_time: {}, thread_active_count: {}".format(
            #         infered_class,
            #         infered_server_confid,
            #         str(infer_used_time)[:5], threading.active_count())))
            # self.save_sample_image(temp_cropped_image_file_full_name,
            #                        object_detection_timeline_item.original_timestamp,
            #                        infered_class, infered_server_confid,
            #                        full_image_frame_base64_encode_text, "qua_")
            # if os.path.isfile(temp_cropped_image_file_full_name) or os.path.islink(temp_cropped_image_file_full_name):
            #     os.unlink(temp_cropped_image_file_full_name)
            self.logger.debug(
                f"inferencer: infered_class: {infered_class}, infered_confid: {infered_confid}")
        except Exception as e:
            self.logger.exception(
                f"inferencer: exception raised: {e}")
            infered_class = 'background'
            infered_confid = 0.0
        return infered_class, infered_confid

    def start_inference_image_from_qua_models(self, base64_image_data_text: str,
                                              callback: Callable[[tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]], None]):
        # put the work into thread pool
        fut = Inferencer.executor.submit(
            self.inference_image_from_qua_models, base64_image_data_text)
        fut.add_done_callback(lambda future: callback(future.result()))
