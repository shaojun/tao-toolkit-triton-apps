import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
from http import HTTPStatus
import io
import json
import logging
import os
import time
from typing import Callable, Literal
import uuid
# from PIL import Image
import cv2
import numpy as np
import requests
from openai import OpenAI


class Inferencer:
    executor = ThreadPoolExecutor(max_workers=15)

    def __init__(self, logger: logging.Logger, logger_str_prefix: str = ""):
        self.logger = logger
        self.logger_str_prefix = logger_str_prefix

    def inference_image_file_from_qua_models(
            self,
            file_full_path: str,
            model_name: Literal['ebicycle', 'gastank', 'battery']) -> tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]:
        # get base64 image data
        with open(file_full_path, "rb") as image_file:
            base64_image_data_text = base64.b64encode(
                image_file.read()).decode('ascii')
            return self.inference_image_from_qua_classification_models(base64_image_data_text, model_name)

    def start_inference_image_file_from_qua_models(
            self,
            file_full_path: str,
            model_name: Literal['ebicycle', 'gastank', 'battery'],
            callback: Callable[[tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]], None]):
        # put the work into thread pool
        fut = Inferencer.executor.submit(
            self.inference_image_file_from_qua_models, file_full_path, model_name)
        fut.add_done_callback(lambda future: callback(future.result()))

    def inference_image_from_qua_detection_models(
            self,
            base64_image_data_text: str,
            service_url: str = "http://36.153.41.19:18091/detect_images") -> list[dict]:
        """
        @param base64_image_data_text: base64 encoded image data
        @param service_url: service url
        @return: list of dict, each dict has keys: name, conf, image_base64_encoded_text. The name could be: name=='ebicycle' or name=='battery' or name=='gastank' and others.
        """
        def scale_and_pad(image: np.ndarray, dsize, padding_value=0):
            def pad(image: np.ndarray, pt, pl, pb, pr, padding_value):
                image = np.pad(image, ((pt, pb), (pl, pr), (0, 0)),
                               "constant", constant_values=padding_value)
                return image

            def scale(image: np.ndarray, ratio):
                height, width, _ = image.shape
                image = cv2.resize(
                    image, (int(ratio * width), int(ratio * height)))
                return image
            w, h = dsize
            height, width, _ = image.shape
            if width == w and height == h:
                return image
            elif width < w and height < h:
                new_image = np.full((h, w, 3), padding_value, dtype=np.uint8)
                paste_top = (h - height) // 2
                paste_left = (w - width) // 2
                new_image[paste_top:paste_top + height,
                          paste_left:paste_left + width] = image
                return new_image
            else:
                ratio = min(w / width, h / height)
                scaled_image = scale(image, ratio)
                height, width, _ = scaled_image.shape
                pt, pl = (h - height) // 2, (w - width) // 2
                pb, pr = h - height - pt, w - width - pl
                new_image = pad(scaled_image, pt, pl, pb, pr, padding_value)
                return new_image

        def make_divisible_by_2(x1, x2):
            """
            判断 (x2 - x1) 是否能被 2 整除，如果不能则减一个最小的数使其能被 16 整除。

            Args:
                x1 (int): 第一个数
                x2 (int): 第二个数

            Returns:
                int: 修正后的 x2 值
            """
            diff = x2 - x1
            subtract = 0
            if diff % 2 != 0:
                subtract = diff % 2
            return subtract

        def base64_image_data_to_cv2_img(base64_image_data_text: str):
            nparr = np.frombuffer(base64.b64decode(
                base64_image_data_text), np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            return img
        try:
            if service_url is None:
                service_url = "http://36.153.41.19:18091/detect_images"
            infer_server_url = service_url
            infer_start_time = time.time()
            original_image = base64_image_data_to_cv2_img(
                base64_image_data_text)
            processed_image = scale_and_pad(original_image, (1280, 1280))
            _, buffer = cv2.imencode('.jpg', processed_image)
            processed_base64_image_data_text = base64.b64encode(
                buffer).decode('utf-8')
            data = {'input_image': processed_base64_image_data_text,
                    'confidence_hold': 0.35, 'iou_hold': 0.35}
            response = requests.post(infer_server_url, json=data, timeout=5)
            if response.status_code != 200:
                raise Exception(
                    f"HTTP response status code is not 200: {response.status_code}, content: {response.text}")
            t1 = time.time()
            infer_used_time_by_ms = (t1 - infer_start_time) * 1000
            height, width = processed_image.shape[:2]
            targets = []
            for item in response.json():
                name = item['name']
                conf = item['conf']
                label = item['label']
                # print(f"Name: {name}, Conf: {conf}")
                x, y, w, h = map(float, label)
                # with open(labelfile, 'a') as f:
                #    f.write(f"{name}, {x}, {y}, {w}, {h}\n")
                x1 = int((x - w / 2) * width)
                y1 = int((y - h / 2) * height)
                x2 = int((x + w / 2) * width)
                y2 = int((y + h / 2) * height)
                coords = (x1, y1, x2, y2)
                targets.append({'name': name, 'coords': coords, 'conf': conf})
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, qua detection with result: {targets}, infer_used_time_by_ms: {infer_used_time_by_ms}")
            return_results: list[dict] = []
            if targets:
                kk = 0
                for target in targets:
                    conf = target['conf']
                    name = target['name']
                    coords = target['coords']
                    (x1, y1, x2, y2) = coords
                    if (x2 - x1) > 20 and (y2 - y1) > 20 and x1 >= 0 and x2 >= 0 and y2 >= 0 and y1 >= 0:
                        diff_x = make_divisible_by_2(x1, x2)
                        diff_y = make_divisible_by_2(y1, y2)

                        target_image = processed_image[y1:y2 -
                                                       diff_y, x1:x2 - diff_x]
                        if target_image.size > 10:
                            # 使用内存中的图像数据进行编码
                            _, buffer = cv2.imencode('.jpg', target_image)
                            encoded_temp_image = base64.b64encode(
                                buffer).decode('utf-8')
                            return_results.append(
                                {'name': name, 'conf': conf, 'coords': coords, 'image_base64_encoded_text': encoded_temp_image})
                            kk += 1
            return return_results
        except Exception as e:
            self.logger.exception(
                f"{self.logger_str_prefix}, inferencer: qua detection, exception raised: {e}")
            return []

    def inference_image_from_qua_classification_models(
            self,
            base64_image_data_text: str,
            model_name: Literal['ebicycle', 'gastank', 'battery'],
            service_url: str = "http://36.153.41.19:18090/detect_images") -> tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]:
        try:
            if service_url is None:
                service_url = "http://36.153.41.19:18090/detect_images"
            infer_server_url = service_url
            infer_start_time = time.time()
            data = {'input_image': base64_image_data_text,
                    'confidence_hold': 0.35,
                    'name': model_name}

            http_response = requests.post(
                infer_server_url, json=data, timeout=5)
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
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, qua with model name: {model_name}, infered_class: {infered_class}, infered_confid: {infered_confid}, infer_used_time_by_ms: {infer_used_time_by_ms}")
        except Exception as e:
            self.logger.exception(
                f"{self.logger_str_prefix}, inferencer: qua, exception raised: {e}")
            infered_class = 'background'
            infered_confid = 0.0
        return infered_class, infered_confid

    def start_inference_image_from_qua_models(
            self,
            base64_image_data_text: str,
            model_name: Literal['ebicycle', 'gastank', 'battery'],
            callback: Callable[[tuple[Literal['bicycle', 'electric_bicycle', 'background', 'gastank', 'battery'], float]], None]):
        # put the work into thread pool
        fut = Inferencer.executor.submit(
            self.inference_image_from_qua_classification_models, base64_image_data_text, model_name)
        fut.add_done_callback(lambda future: callback(future.result()))

    def inference_discrete_images_from_ali_qwen_vl_plus_model(
            self,
            image_file_full_path_or_base64_str_list: list[str],
            model_name: Literal['qwen-vl-max-0809',
                                'qwen-vl-plus-0809',
                                'qwen-vl-plus'] = 'qwen-vl-max-0809',
            user_prompt: str = None,
            system_prompt: str = None) -> dict:
        """
        from exp, 3 images would take 4 seconds to get response back.
        @param image_file_full_path_list: list of image file full path, from limited testing, input list MAX contain 3 images
        """
        import os
        import dashscope
        from http import HTTPStatus
        # only take first 3 images
        image_file_full_path_or_base64_str_list = image_file_full_path_or_base64_str_list[:3]

        if len(image_file_full_path_or_base64_str_list) > 3:
            raise ValueError("input list MAX contain 3 images")
        is_image_in_base64_str = False

        # make sure all path exists
        for image_file_full_path in image_file_full_path_or_base64_str_list:
            # determine if the input is base64 string or file path
            if image_file_full_path_or_base64_str_list[0].startswith("data:image"):
                is_image_in_base64_str = True
                # convert base64 string to image file
                for i in range(len(image_file_full_path_or_base64_str_list)):
                    img_data = image_file_full_path_or_base64_str_list[i]
                    img_data = img_data.split(",")[1]
                    img_data = base64.b64decode(img_data)
                    with open(f"/tmp/{uuid.uuid4()}.jpg", "wb") as f:
                        f.write(img_data)
                        image_file_full_path_or_base64_str_list[i] = f.name
            elif not os.path.exists(image_file_full_path):
                raise FileNotFoundError(
                    f"image file not found: {image_file_full_path}")

        default_system_prompt = "你是一个识别车辆的专家,能区分出这3类成人车辆: 自行车,电瓶车,摩托车. 如果你没有看到这3类车辆,或者不太确定,请回答:其它. \n切记,一定以json格式回答,如```{\"vehicle_type\":\"自行车\",\"reason\":\"\"}```"
        if system_prompt is None:
            system_prompt = default_system_prompt

        default_user_prompt = "请查看图片并回答和解释原因"
        if user_prompt is None:
            user_prompt = default_user_prompt
        try:
            messages = [
                {
                    "role": "system",
                    "content": [
                        {"text": system_prompt}]},

                {
                    "role": "user",
                    "content": [
                        {"image": f"file://{i}"} for i in image_file_full_path_or_base64_str_list]}
            ]

            messages[1]["content"].append({"text": user_prompt})
            infer_start_time = time.time()
            response = dashscope.MultiModalConversation.call(
                model=model_name,
                messages=messages,
                output_format="json",
            )

            infer_used_time_by_ms = (time.time() - infer_start_time) * 1000
            if response.status_code == HTTPStatus.OK:
                try:
                    raw_result = response.output.choices[0].message.content
                    self.logger.debug(
                        f"{self.logger_str_prefix}, inferencer, qwen_vl_plus, raw_result: {raw_result}, infer_used_time_by_ms: {infer_used_time_by_ms}")
                    print(
                        f"{datetime.datetime.now()} {self.logger_str_prefix} - qwen_vl_plus images infer used time(by ms): {infer_used_time_by_ms}")
                    unformat_json_text = raw_result[0]["text"]
                except Exception as e:
                    print(
                        f"error for parsing basic structure from llm response: {e}")
                    raise e
                # find the index of first char: { or [
                start_index = unformat_json_text.find("{")
                if start_index == -1:
                    start_index = unformat_json_text.find("[")
                    last_index = unformat_json_text.rfind("]")
                    if start_index == -1 or last_index == -1:
                        self.logger.debug(
                            f"{self.logger_str_prefix}, inferencer, qwen_vl_plus images, error for parsing json structure from llm response")
                        return None
                        raise ValueError(
                            "error for parsing json structure from llm response")
                else:
                    last_index = unformat_json_text.rfind("}")
                    if last_index == -1:
                        self.logger.debug(
                            f"{self.logger_str_prefix}, inferencer, qwen_vl_plus images, error for parsing json structure from llm response")
                        return None
                        raise ValueError(
                            "error for parsing json structure from llm response")
                json_str = unformat_json_text[start_index:last_index+1]
                js = json.loads(json_str)
                return js
            else:
                self.logger.debug(
                    f"{self.logger_str_prefix}, inferencer, qwen_vl_plus images, HTTPStatus NOT OK, raw_result: {response.output}, infer_used_time_by_ms: {infer_used_time_by_ms}")
                # The error code.
                print(f"{str(datetime.datetime.now())} {self.logger_str_prefix}, inferencer, qwen_vl_plus images, HTTPStatus NOT OK: {response.code} - {response.message}")
        finally:
            if is_image_in_base64_str:
                for i in image_file_full_path_or_base64_str_list:
                    os.remove(i)

    def inference_video_by_convert_from_image_frames_from_ali_qwen_vl_model(
            self,
            image_file_full_path_or_base64_str_list: list[str],
            model_name: Literal['qwen-vl-max',
                                'qwen-vl-plus-0809'] = 'qwen-vl-plus-0809',
            enable_video_convert: bool = False,
            enable_base64_image_to_local_file: bool = True,
            user_prompt: str = None,
            system_prompt: str = None) -> dict:
        """
        infer with a video which composed by images, for 5 images of resolution 360*600, the cost is: "usage": {"input_tokens": 946, "output_tokens": 14, "video_tokens": 858}}
        @param image_file_full_path_or_base64_str_list:
        @param model_name: model name, default is qwen-vl-plus-0809
        @param enable_video_convert: enable video convert or not, default is True
        @param duplicate_image_count_for_make_video: when original image count is less than 10, for improve the accuracy, will duplicate original images for making video, default is 3, that say 3 images will be duplicated to 9 images
        """
        import os
        import dashscope
        import cv2
        from http import HTTPStatus
        original_image_count = len(image_file_full_path_or_base64_str_list)
        # try append to make sure the input list has at least 3 images, guess may improve the perf??
        if len(image_file_full_path_or_base64_str_list) == 1:
            image_file_full_path_or_base64_str_list.append(
                image_file_full_path_or_base64_str_list[0])
        if len(image_file_full_path_or_base64_str_list) == 2:
            image_file_full_path_or_base64_str_list.append(
                image_file_full_path_or_base64_str_list[1])

        is_image_in_base64_str = False
        if enable_base64_image_to_local_file and image_file_full_path_or_base64_str_list[0].startswith("data:image"):
            is_image_in_base64_str = True
            # convert base64 string to image file
            for i in range(len(image_file_full_path_or_base64_str_list)):
                img_data = image_file_full_path_or_base64_str_list[i]
                img_data = img_data.split(",")[1]
                img_data = base64.b64decode(img_data)
                with open(f"/tmp/{uuid.uuid4()}.jpg", "wb") as f:
                    f.write(img_data)
                    image_file_full_path_or_base64_str_list[i] = f.name

        def images_to_video(image_file_full_path_list: list[str], output_video_file_full_path: str):
            # get the max hight and width by loop all images
            max_height = 0
            max_width = 0
            for p in image_file_full_path_list:
                img = cv2.imread(p)
                height, width, _ = img.shape
                if height > max_height:
                    max_height = height
                if width > max_width:
                    max_width = width
            size = (max_width, max_height)
            frame_rate = 4
            video_writer = cv2.VideoWriter(
                output_video_file_full_path, cv2.VideoWriter_fourcc(*'mp4v'), frame_rate, (max_width, max_height))

            duplicate_image_count_for_make_video = int(3*frame_rate /
                                                       len(image_file_full_path_list))
            for p in image_file_full_path_list:
                try:
                    img = cv2.imread(p)
                    # resize image
                    img = cv2.resize(img, size)
                    for i in range(duplicate_image_count_for_make_video):
                        video_writer.write(img)
                finally:
                    if is_image_in_base64_str:
                        os.remove(p)

            video_writer.release()
            print(f"{datetime.datetime.now()} {self.logger_str_prefix} - video file created with max hight: {max_height}, max width: {max_width}, total frames: {len(image_file_full_path_list) * duplicate_image_count_for_make_video}")

        default_system_prompt = "你是一个识别车辆的专家,能区分出这3类成人车辆: 自行车,电瓶车,摩托车. 如果你没有看到这3类车辆,或者不太确定,请回答:其它. \n切记,一定以json格式回答,如```{\"vehicle_type\":\"自行车\",\"reason\":\"\"}```"
        if system_prompt is None:
            system_prompt = default_system_prompt

        default_user_prompt = "请查看图片并回答和解释原因"
        if user_prompt is None:
            user_prompt = default_user_prompt
        try:
            infer_start_time = time.time()
            messages = [
                {
                    "role": "system",
                    "content": [
                        {"text": system_prompt}]},
                {
                    "role": "user",
                    "content": [
                        # 以视频文件传入
                        # {"video": "https://cloud.video.taobao.com/vod/S8T54f_w1rkdfLdYjL3S5zKN9CrhkzuhRwOhF313tIQ.mp4"},
                        # 或以图片列表形式传入
                        {"video": [
                            f"file://{i}" for i in image_file_full_path_or_base64_str_list]},
                        {"text": user_prompt}
                    ]
                }
            ]

            response = dashscope.MultiModalConversation.call(
                model=model_name,
                messages=messages,
                api_key=os.getenv("DASHSCOPE_API_KEY"),
                result_format='json'
            )

            print(
                f"{datetime.datetime.now()} {self.logger_str_prefix} - qwen video infer used time(by ms): {(time.time() - infer_start_time) * 1000} - {original_image_count} original images - {len(image_file_full_path_or_base64_str_list)} images - {response}")
        except Exception as e:
            print(f"{str(datetime.datetime.now())} {self.logger_str_prefix}, inferencer, qwen video, dashscope call exception: {e}")
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, qwen video, dashscope call exception: {e}")
            return None
        finally:
            for p in image_file_full_path_or_base64_str_list:
                if is_image_in_base64_str:
                    os.remove(p)
            pass

        infer_used_time_by_ms = (time.time() - infer_start_time) * 1000
        if response.status_code == HTTPStatus.OK:
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, qwen video, raw_result: {response}, {original_image_count} original images - {len(image_file_full_path_or_base64_str_list)} images - infer_used_time_by_ms: {infer_used_time_by_ms}")
            try:
                raw_result = response.output.choices[0].message.content
                unformat_json_text = raw_result[0]["text"]
            except Exception as e:
                print(
                    f"{datetime.datetime.now()} {self.logger_str_prefix} - error for parsing basic structure from llm response: {e}")
                self.logger.debug(
                    f"{self.logger_str_prefix}, inferencer, qwen video, error for parsing basic structure from llm response: {e}")
                return None
            # find the index of first char: { or [
            start_index = unformat_json_text.find("{")
            if start_index == -1:
                start_index = unformat_json_text.find("[")
                last_index = unformat_json_text.rfind("]")
                if start_index == -1 or last_index == -1:
                    self.logger.debug(
                        f"{self.logger_str_prefix}, inferencer, qwen video, error for parsing json structure from llm response")
                    return None
                    raise ValueError(
                        "error for parsing json structure from llm response")
            else:
                last_index = unformat_json_text.rfind("}")
                if last_index == -1:
                    self.logger.debug(
                        f"{self.logger_str_prefix}, inferencer, qwen video, error for parsing json structure from llm response")
                    return None
                    raise ValueError(
                        "error for parsing json structure from llm response")
            json_str = unformat_json_text[start_index:last_index+1]
            js = json.loads(json_str)
            # print(js)
            return js
        else:
            print(f"{str(datetime.datetime.now())} {self.logger_str_prefix}, inferencer, qwen video, HTTPStatus NOT OK: {response.code} - {response.message}")
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, qwen video, HTTPStatus NOT OK, raw_result: {response.output}, {original_image_count} original images - {len(image_file_full_path_or_base64_str_list)} images - infer_used_time_by_ms: {infer_used_time_by_ms}")
            return None
            # print(response.code)  # The error code.
            # print(response.message)  # The error message.

    def inference_video_by_convert_from_image_frames_from_glm_4v_model(
            self,
            image_file_full_path_or_base64_str_list: list[str],
            user_prompt: str = None,
            system_prompt: str = None):
        """
        infer with a video which composed by images
        @param image_file_full_path_or_base64_str_list: list of image file full path, will be converted to video file, from exp, >=10 frames would be a good start of accuracy
        @param model_name: model name, default is glm-4v
        @param enable_video_convert: enable video convert or not, default is True
        @param duplicate_image_count_for_make_video: when original image count is less than 10, for improve the accuracy, will duplicate original images for making video, default is 3, that say 3 images will be duplicated to 9 images
        """
        import os
        from zhipuai import ZhipuAI
        import cv2
        is_image_in_base64_str = False
        if image_file_full_path_or_base64_str_list[0].startswith("data:image"):
            is_image_in_base64_str = True
            # convert base64 string to image file
            for i in range(len(image_file_full_path_or_base64_str_list)):
                img_data = image_file_full_path_or_base64_str_list[i]
                img_data = img_data.split(",")[1]
                img_data = base64.b64decode(img_data)
                with open(f"/tmp/{uuid.uuid4()}.jpg", "wb") as f:
                    f.write(img_data)
                    image_file_full_path_or_base64_str_list[i] = f.name

        def images_to_video(image_file_full_path_list: list[str], output_video_file_full_path: str):
            # get the max hight and width by loop all images
            max_height = 0
            max_width = 0
            for p in image_file_full_path_list:
                img = cv2.imread(p)
                height, width, _ = img.shape
                if height > max_height:
                    max_height = height
                if width > max_width:
                    max_width = width
            size = (max_width, max_height)
            frame_rate = 4
            video_writer = cv2.VideoWriter(
                output_video_file_full_path, cv2.VideoWriter_fourcc(*'mp4v'), frame_rate, (max_width, max_height))

            duplicate_image_count_for_make_video = int(3*frame_rate /
                                                       len(image_file_full_path_list))
            for p in image_file_full_path_list:
                try:
                    img = cv2.imread(p)
                    # resize image
                    img = cv2.resize(img, size)
                    for i in range(duplicate_image_count_for_make_video):
                        video_writer.write(img)
                finally:
                    if is_image_in_base64_str:
                        os.remove(p)

            video_writer.release()
            print(f"{datetime.datetime.now()} {self.logger_str_prefix} - video file created with max hight: {max_height}, max width: {max_width}, total frames: {len(image_file_full_path_list) * duplicate_image_count_for_make_video}")

        default_system_prompt = "你是一个识别车辆的专家,能区分出这3类成人车辆: 自行车,电瓶车,摩托车. 如果你没有看到这3类车辆,或者不太确定,请回答:其它. \n切记,一定以json格式回答,如```{\"vehicle_type\":\"自行车\",\"reason\":\"\"}```"
        if system_prompt is None:
            system_prompt = default_system_prompt

        default_user_prompt = default_system_prompt + "\n请回答和解释原因"
        if user_prompt is None:
            user_prompt = default_user_prompt

        video_file_full_path: str = None
        try:
            video_file_full_path = f"/tmp/{uuid.uuid4()}.mp4"
            images_to_video(
                image_file_full_path_or_base64_str_list, video_file_full_path)
            with open(video_file_full_path, 'rb') as video_file:
                video_base = base64.b64encode(
                    video_file.read()).decode('utf-8')
            client = ZhipuAI(api_key=os.getenv(
                "zhipuai_API_KEY"))  # 填写您自己的APIKey
            infer_start_time = time.time()

            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "video_url",
                         # 以视频文件传入
                         # {"video": "https://cloud.video.taobao.com/vod/S8T54f_w1rkdfLdYjL3S5zKN9CrhkzuhRwOhF313tIQ.mp4"},
                         # 或以图片列表形式传入
                         "video_url": {
                             "url": video_base
                         }},
                        {"type": "text", "text": "你能看到这3类车辆吗? 自行车,电瓶车,摩托车. 如果你没有看到这3类车辆,或者不太确定,请回答:其它. do response to me with json:```{\"vehicle_type\":\"自行车\",\"reason\":\"\"}```"}
                    ]
                }
            ]

            response = client.chat.completions.create(
                model="glm-4v-plus",
                messages=messages,
            )

            print(
                f"{datetime.datetime.now()} {self.logger_str_prefix} - glm4v video infer used time(by ms): {(time.time() - infer_start_time) * 1000}")
        finally:
            for p in image_file_full_path_or_base64_str_list:
                if is_image_in_base64_str and os.path.exists(p):
                    os.remove(p)
            pass

        infer_used_time_by_ms = (time.time() - infer_start_time) * 1000
        print(response.choices[0].message)
        return None
        if response.status_code == HTTPStatus.OK:
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, glm4v video, raw_result: {response}, infer_used_time_by_ms: {infer_used_time_by_ms}")
            # print(response)
            if video_file_full_path is not None:
                os.remove(video_file_full_path)
            try:
                raw_result = response.output.choices[0].message.content
                unformat_json_text = raw_result[0]["text"]
            except Exception as e:
                print(
                    f"{datetime.datetime.now()} {self.logger_str_prefix} - error for parsing basic structure from llm response: {e}")
                self.logger.debug(
                    f"{self.logger_str_prefix}, inferencer, glm4v video, error for parsing basic structure from llm response: {e}")
                return None
            # find the index of first char: { or [
            start_index = unformat_json_text.find("{")
            if start_index == -1:
                start_index = unformat_json_text.find("[")
                last_index = unformat_json_text.rfind("]")
                if start_index == -1 or last_index == -1:
                    self.logger.debug(
                        f"{self.logger_str_prefix}, inferencer, glm4v video, error for parsing json structure from llm response")
                    return None
                    raise ValueError(
                        "error for parsing json structure from llm response")
            else:
                last_index = unformat_json_text.rfind("}")
                if last_index == -1:
                    self.logger.debug(
                        f"{self.logger_str_prefix}, inferencer, glm4v video, error for parsing json structure from llm response")
                    return None
                    raise ValueError(
                        "error for parsing json structure from llm response")
            json_str = unformat_json_text[start_index:last_index+1]
            js = json.loads(json_str)
            # print(js)
            return js
        else:
            print(f"{str(datetime.datetime.now())} {self.logger_str_prefix}, inferencer, glm4v video, HTTPStatus NOT OK: {response.code} - {response.message}")
            self.logger.debug(
                f"{self.logger_str_prefix}, inferencer, glm4v video, HTTPStatus NOT OK, raw_result: {response.output}, infer_used_time_by_ms: {infer_used_time_by_ms}")
            if video_file_full_path is not None:
                os.remove(video_file_full_path)
            return None
            # print(response.code)  # The error code.
            # print(response.message)  # The error message.
