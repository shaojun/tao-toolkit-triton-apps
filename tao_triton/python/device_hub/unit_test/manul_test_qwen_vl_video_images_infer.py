import base64
import logging
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer


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
# result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
#     full_path_list,"这些图片里都是同一个车辆,你能回答这个车辆是什么类型吗?")
# 你看到了什么类型的车辆, 并简单分析车辆的特征?请从以下几种选择里回答:未知,自行车,电瓶车,摩托车.\n切记,不要回答其它任何内容
result = inferencer.inference_video_by_convert_from_image_frames_from_ali_qwen_vl_model(
    full_path_list, model_name="qwen-vl-plus-0809", enable_video_convert=True, user_prompt="回答你看到了什么类型的车辆, 并从车辆特征方面解释原因")
print(result)
