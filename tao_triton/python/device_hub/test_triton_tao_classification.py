from tao_triton.python.device_hub import base64_tao_client
from PIL import Image
import base64
import io
import datetime
import os
import re
import argparse
import glob

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--enable-random-input-and-visualize-output-mode',
                        type=str,
                        default=False,
                        help="random content of images stayed in single folder, and output them to different sub folders with class name",
                        required=False)
    parser.add_argument('-a', '--enable-assess-mode',
                        type=str,
                        default=True,
                        help="images must be pre-classified and put into sub folders with class name",
                        required=False)
    parser.add_argument('--input-images-folder-path',
                        type=str,
                        default=os.path.join(os.getcwd(), "/home/shao/Downloads/val"),
                        help="Path to the folder of images for classifying, if -r enabled, the single folder",
                        required=False)
    parser.add_argument('--output-image-classes-folder-path',
                        type=str,
                        default=os.path.join(
                            os.getcwd(), "output_image_classes"),
                        help="Path to the folder of classified images with sub folder of class",
                        required=False)
    FLAGS = parser.parse_args()
    FLAGS.enable_random_input_and_visualize_output_mode

    #3060GPU machine ip:  36.153.41.18:18000
    infer_server_url = "localhost:8000"
    classes = ['background', 'bicycle', 'electric_bicycle', 'people']
    elenet_four_classes_model_statistics = {}
    if FLAGS.enable_assess_mode:
        classfolders = [f.path for f in os.scandir(
            FLAGS.input_images_folder_path) if f.is_dir()]
        for classfolder in list(classfolders):
            files_count = 0
            classfoldername = os.path.basename(classfolder)
            elenet_four_classes_model_statistics[classfoldername] = {'classfoldername': classfolder, 'files_count': 0,
                                                                     'total_infer_times_by_seconds': 0}
            # statistics[classfolder][] = 0
            for file in os.listdir(classfolder):
                with open(os.path.join(classfolder, file), "rb") as image_file:
                    elenet_four_classes_model_statistics[classfoldername]['files_count'] += 1
                    encoded_string = base64.b64encode(image_file.read())
                    full_image_base64_encoded_text = encoded_string.decode(
                        'ascii')
                    t0 = datetime.datetime.now()
                    # 推理服务器36.153.41.18:18000
                    infer_results = base64_tao_client.infer(False, False, False,
                                                            "elenet_four_classes_230330_tao", "",
                                                            1, "",
                                                            False, infer_server_url, "HTTP", "Classification",
                                                            os.path.join(
                                                                os.getcwd(), "outputs"),
                                                            [full_image_base64_encoded_text])
                    infer_used_time = (datetime.datetime.now() - t0).total_seconds()
                    elenet_four_classes_model_statistics[classfoldername][
                        'total_infer_times_by_seconds'] += infer_used_time
                    # sample: (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(0)=bicycle, 0.4476(1)=electric_bicycle
                    # the `0.4476(1)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
                    # which is less than 50%, so it's a bicycle, should not trigger alarm.
                    m = re.search('\d\.\d+\(\d\)', infer_results)
                    if m and m.group(0):
                        infer_class = classes[int(m.group(0)[-2])]
                        infer_server_confid = float(
                            m.group(0)[0:len(m.group(0)) - 4])
                        print('{} is classfied as {} with confid: {}'.format(
                            file, infer_class, infer_server_confid))
                        if infer_class in elenet_four_classes_model_statistics[classfoldername].keys():
                            elenet_four_classes_model_statistics[classfoldername][infer_class] += 1
                        else:
                            elenet_four_classes_model_statistics[classfoldername][infer_class] = 1

        elenet_two_classes = ['bicycle', 'electric_bicycle']
        elenet_two_classes_model_statistics = {}
        for classfolder in list(classfolders):
            files_count = 0
            classfoldername = os.path.basename(classfolder)
            elenet_two_classes_model_statistics[classfoldername] = {'classfoldername': classfolder, 'files_count': 0}
            # statistics[classfolder][] = 0
            for file in os.listdir(classfolder):
                with open(os.path.join(classfolder, file), "rb") as image_file:
                    elenet_two_classes_model_statistics[classfoldername]['files_count'] += 1
                    encoded_string = base64.b64encode(image_file.read())
                    full_image_base64_encoded_text = encoded_string.decode(
                        'ascii')
                    # 推理服务器36.153.41.18:18000
                    infer_results = base64_tao_client.infer(False, False, False,
                                                            "bicycletypenet_tao", "",
                                                            1, "",
                                                            False, infer_server_url, "HTTP", "Classification",
                                                            os.path.join(
                                                                os.getcwd(), "outputs11"),
                                                            [full_image_base64_encoded_text])
                    # sample: (localConf:0.850841)infer_results: temp_infer_image_files\0.jpg, 0.5524(0)=bicycle, 0.4476(1)=electric_bicycle
                    # the `0.4476(1)=electric_bicycle`  means the infer server is 0.4476 sure the object is electric_bicycle
                    # which is less than 50%, so it's a bicycle, should not trigger alarm.
                    m = re.search('\d\.\d+\(\d\)', infer_results)
                    if m and m.group(0):
                        infer_class = elenet_two_classes[int(m.group(0)[-2])]
                        infer_server_confid = float(
                            m.group(0)[0:len(m.group(0)) - 4])
                        print('{} is classfied as {} with confid: {}'.format(
                            file, infer_class, infer_server_confid))
                        if infer_class in elenet_two_classes_model_statistics[classfoldername].keys():
                            elenet_two_classes_model_statistics[classfoldername][infer_class] += 1
                        else:
                            elenet_two_classes_model_statistics[classfoldername][infer_class] = 1
        print('elenet_two_classes_model_statistics:')
        for key in elenet_two_classes_model_statistics.keys():
            folderStats = elenet_two_classes_model_statistics[key]
            positive_count = 0
            if key not in folderStats.keys():
                positive_count = 0
            else:
                positive_count = folderStats[key]
            print(
                '   accurate: {}, detail: {}'.format(int(positive_count) / int(folderStats['files_count']),
                                                     folderStats))
        print('')
        print('elenet_four_classes_model_statistics:')
        for key in elenet_four_classes_model_statistics.keys():
            folderStats = elenet_four_classes_model_statistics[key]
            print(
                '   accurate: {}, detail: {}, total_infer_times_by_ms: {}'.format(
                    int(folderStats[key]) / int(folderStats['files_count']),
                    folderStats,
                    1000*int(folderStats['total_infer_times_by_seconds']) / int(folderStats['files_count'])))
