import base64
import logging
from tao_triton.python.device_hub.utility.infer_image_from_model import Inferencer


inferencer = Inferencer(logging.getLogger("dummy"))
full_path_list = ["/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/1.png",
                  #   "/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/2.png",
                  #   "/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/3.png",
                  #   "/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/4.png"
                  ]
# convert image to base64 encoded string
image_file_base64_encoded_string_list = []
for full_path in full_path_list:
    with open(full_path, "rb") as image_file:
        image_file_base64_encoded_string_list.append(
            "data:image,"+base64.b64encode(image_file.read()).decode("utf-8"))
# result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
#     full_path_list)
# # 你看到了什么类型的车辆, 并简单分析车辆的特征?请从以下几种选择里回答:未知,自行车,电瓶车,摩托车.\n切记,不要回答其它任何内容
# print(result)
# result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
#     ["/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/qua_0.94___2024_1018_0756_57_880___0756_58_512.jpg"],
# )
# print(result)

# result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
#     ["/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/bicycle_move_out_scene1/qua_bicycle_1.0___2024_1018_0756_58_849___0756_59_556.jpg"],
# )
# print(result)

result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
    ["/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/real/people_with_backpack_1/video_frame_3_localeb_1___2024_1028_1759_48_092___1759_48_092.jpg"],
    model_name="qwen2-vl-7b-instruct"
)
print(result)

result = inferencer.inference_discrete_images_from_ali_qwen_vl_plus_model(
    ["/home/shao/tao-toolkit-triton-apps/tao_triton/python/device_hub/unit_test/assets/real/people_with_backpack_1/video_frame_3_localeb_1___2024_1028_1759_48_092___1759_48_092.jpg"],
    model_name="qwen-vl-plus-0809"
)
print(result)
