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
            base64.b64encode(image_file.read()).decode("utf-8"))
result = inferencer.inference_image_from_qua_detection_models(
    image_file_base64_encoded_string_list[0])
simplified_result :list[dict] = []
for r in result:
    print(f"detect obj: {r['name']} with conf: {r['conf']}")
    if r["name"] == "ebicycle" and r["conf"] >= 0.1:
        classification_result = inferencer.inference_image_from_qua_classification_models(
            r["image_base64_encoded_text"], r["name"])
        print(f"    eb further classified: {classification_result}")
