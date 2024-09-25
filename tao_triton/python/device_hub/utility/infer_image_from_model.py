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
            if http_response.status_code != 200:
                raise Exception(
                    f"HTTP response status code is not 200: {http_response.status_code}")
            t1 = time.time()
            infer_used_time_by_ms = (t1 - infer_start_time) * 1000
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
                f"inferencer, qua, infered_class: {infered_class}, infered_confid: {infered_confid}, infer_used_time_by_ms: {infer_used_time_by_ms}")
        except Exception as e:
            self.logger.exception(
                f"inferencer: qua, exception raised: {e}")
            infered_class = 'background'
            infered_confid = 0.0
        return infered_class, infered_confid

    def start_inference_image_from_qua_models(self, base64_image_data_text: str,
                                              callback: Callable[[tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]], None]):
        # put the work into thread pool
        fut = Inferencer.executor.submit(
            self.inference_image_from_qua_models, base64_image_data_text)
        fut.add_done_callback(lambda future: callback(future.result()))

    def inference_image_from_ali_qwen_models(self, base64_image_data_text: str) -> tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]:
        def get_response(base64_image):
            os.environ["DASHSCOPE_API_KEY"] = "sk-acc1da6bad004b7bbc127a29961cbc38"
            api_key = os.getenv("DASHSCOPE_API_KEY")
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            payload = {
                "model": "qwen-vl-max",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            },
                            {
                                "type": "text",
                                "text": "图片中是否包含以下几类物体中的一种: 1.成人电动车 2.成人电动踏板车 3.摩托车. \n如果确信找到了(注意,自行车,滑板车需要除外),则回答: {\"found\":true,\"actual\":\"\"}, 否则回答: {\"found\":false,\"actual\":\"\"}. 其中actual字段用于简单描述图片中的实际内容. 切记,不要回复其它任何内容"
                            }
                        ]
                    }
                ]
            }
            payload = {
                "model": "qwen-vl-max",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            },
                            {
                                "type": "text",
                                "text": "determine if contains: 1.electric bicycle 2.motocycle. \nif found(NOTE, the bicycle should be excluded),then answer: {\"found\":true,\"actual\":\"\"}, otherwise answer: {\"found\":false,\"actual\":\"\"}. pls put the actual content you've seen into field: `actual`"
                            }
                        ]
                    }
                ]
            }
            response = requests.post(
                "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions", headers=headers, json=payload)
            return response.json()
        try:
            infer_start_time = time.time()
            # raw response sample:
            """
            {
  "choices": [
    {
      "message": {
        "content": "这是一只在天空中飞翔的鹰。它有着广阔的翅膀，正在翱翔于云层之间。这种鸟类通常被认为是力量、自由和雄心壮志的象征，在各种文化中有重要的地位。",
        "role": "assistant"
      },
      "finish_reason": "stop",
      "index": 0,
      "logprobs": None
    }
  ],
  "object": "chat.completion",
  "usage": {
    "prompt_tokens": 1254,
    "completion_tokens": 45,
    "total_tokens": 1299
  },
  "created": 1721732005,
  "system_fingerprint": None,
  "model": "qwen-vl-plus",
  "id": "chatcmpl-13b925d1-ef79-9c15-b890-0079a096d7d3"
}
            """
            raw_response = get_response(base64_image_data_text)
            self.logger.debug(
                f"inferencer, qwen, raw_response: {raw_response}")
            t1 = time.time()
            infer_used_time_by_ms = (t1 - infer_start_time) * 1000
            response_msg_content = raw_response['choices'][0]['message']['content']
            response_msg_content = json.loads(response_msg_content)
            if response_msg_content["found"]:
                infered_class = "electric_bicycle"
                infered_confid = 1.0
            else:
                infered_class = "non_eb"
                infered_confid = 1.0
            reason = response_msg_content["actual"]
            self.logger.debug(
                f"inferencer, qwen, infered_class: {infered_class}, infered_confid: {infered_confid}, infer_used_time_by_ms: {infer_used_time_by_ms}")
        except Exception as e:
            self.logger.exception(
                f"inferencer: qua, exception raised: {e}")
            infered_class = 'error'
            infered_confid = 1.0
        return infered_class, infered_confid, reason
