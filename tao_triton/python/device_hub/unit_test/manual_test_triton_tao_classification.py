import sys
import os
from tao_triton.python.entrypoints import tao_client
from PIL import Image
import base64
import io
import datetime
import os
import shutil
import re
import argparse
import glob
import numpy as np
import shutil

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r', '--enable-random-input-and-visualize-output-mode', type=str, default=False,
        help="random content of images stayed in single folder, and output them to different sub folders with class name",
        required=False)
    parser.add_argument('-a', '--enable-assess-mode',
                        type=str,
                        default=True,
                        help="images must be pre-classified and put into sub folders with class name",
                        required=False)
    parser.add_argument('--input-images-folder-path',
                        type=str,
                        default=os.path.join(
                            os.getcwd(), "/home/kevin/Pictures/testing_split_from_yaoming/test"),
                        help="Path to the folder of images for classifying, if -r enabled, the single folder",
                        required=False)
    parser.add_argument('--output-wrong-classified-images-to-folder-path',
                        type=str,
                        default=os.path.join(
                            os.getcwd(), "test_triton_tao_output_wrongly_classified_images"),
                        help="Path to the folder of wrongly classified images with sub folder of each class",
                        required=False)
    FLAGS = parser.parse_args()
    FLAGS.enable_random_input_and_visualize_output_mode

    if os.path.exists(FLAGS.output_wrong_classified_images_to_folder_path):
        shutil.rmtree(FLAGS.output_wrong_classified_images_to_folder_path)

    temp_image_files_folder_name = "temp_infer_image_files"
    # purge previous temp files
    if os.path.exists(temp_image_files_folder_name):
        for filename in os.listdir(temp_image_files_folder_name):
            file_path = os.path.join(temp_image_files_folder_name, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete temp_image_files_folder %s. Reason: %s' % (
                    file_path, e))
    else:
        os.makedirs(temp_image_files_folder_name)

    # 3060GPU machine ip:  36.153.41.18:18000
    infer_server_url = "36.153.41.18:18000"  # "localhost:8000"
    testing_model_names = [
        # "elenet_four_classes_230417_tao",
        # "elenet_four_classes_230620_tao",
        # "elenet_four_classes_230702_tao",
        "elenet_four_classes_230722_tao"
    ]
    classes = ['background', 'bicycle', 'electric_bicycle', 'people']
    model_statistics = {}
    if FLAGS.enable_assess_mode:
        classfolders = [f.path for f in os.scandir(
            FLAGS.input_images_folder_path) if f.is_dir()]
        for testing_model_name in testing_model_names:
            model_statistics[testing_model_name] = {}
            for single_class_folder_full_path in list(classfolders):
                files_count = 0
                class_folder_simple_name = os.path.basename(
                    single_class_folder_full_path)
                model_statistics[testing_model_name][class_folder_simple_name] = {
                    'image_folder_path': single_class_folder_full_path,
                    'image_class_name': class_folder_simple_name,
                    'image_file_count': 0,
                    'correctly_classified_confid_values': [],
                    'wrong_classified_to_classes_info': {},
                    'total_infer_confid': 0,
                    'total_infer_times_by_seconds': 0}
                for simple_file_name in os.listdir(single_class_folder_full_path):
                    with open(os.path.join(single_class_folder_full_path, simple_file_name), "rb") as image_file:
                        stats = model_statistics[testing_model_name][class_folder_simple_name]
                        stats['image_file_count'] += 1
                        encoded_string = base64.b64encode(image_file.read())
                        full_image_base64_encoded_text = encoded_string.decode(
                            'ascii')
                        import uuid

                        file_ext_name = os.path.splitext(simple_file_name)[1]
                        temp_image_file_full_name = os.path.join(temp_image_files_folder_name,
                                                                 str(uuid.uuid4()) + file_ext_name)
                        temp_image = Image.open(io.BytesIO(base64.decodebytes(
                            full_image_base64_encoded_text.encode('ascii'))))
                        temp_image.save(temp_image_file_full_name)
                        t0 = datetime.datetime.now()
                        try:
                            infer_results = tao_client.callable_main(['-m', testing_model_name,
                                                                      '--mode', 'Classification',
                                                                      '-u', infer_server_url,
                                                                      '--output_path', './',
                                                                      temp_image_file_full_name])
                            infered_class = infer_results[0][0]['infer_class_name']
                            infered_server_confid = infer_results[0][0]['infer_confid']
                        except:
                            infered_class = 'exceptioned'
                            infered_server_confid = 0
                            print('!!!exceptioned in tao_client.callable_main, ')
                        if os.path.isfile(temp_image_file_full_name) or os.path.islink(temp_image_file_full_name):
                            os.unlink(temp_image_file_full_name)

                        infer_used_time = (
                            datetime.datetime.now() - t0).total_seconds()
                        stats['total_infer_times_by_seconds'] += infer_used_time

                        if infered_class == class_folder_simple_name:
                            stats['correctly_classified_confid_values'].append(
                                infered_server_confid)
                            stats['total_infer_confid'] += infered_server_confid
                        else:
                            # wrong_classified_to_classes_info
                            if infered_class in stats['wrong_classified_to_classes_info']:
                                stats['wrong_classified_to_classes_info'][infered_class].append({
                                    'confid': infered_server_confid,
                                    'file_full_path': os.path.join(single_class_folder_full_path, simple_file_name)
                                })
                                # infered_server_confid)
                            else:
                                stats['wrong_classified_to_classes_info'][infered_class] = [
                                    {
                                        'confid': infered_server_confid,
                                        'file_full_path': os.path.join(single_class_folder_full_path, simple_file_name)
                                    }]
        print('\r\n\r\n')
        confid_watch_points = [0, 0.5, 0.7, 0.9]
        # confid_watch_points = [0, 0.6, 0.9]
        for testing_model_name in testing_model_names:
            print('Statistics for model: {}, dataset: {}'.format(testing_model_name, FLAGS.input_images_folder_path))
            for target_class_name in model_statistics[testing_model_name].keys():
                stats = model_statistics[testing_model_name][target_class_name]
                for confid_watch_point in confid_watch_points:
                    False_Negative_times = 0
                    '''re-loop all class statistics to extract the wrong classified info to target_class_name'''
                    for inner_class_name in model_statistics[testing_model_name].keys():
                        for wrong_info in model_statistics[testing_model_name][inner_class_name]['wrong_classified_to_classes_info'].get(target_class_name, []):
                            if wrong_info['confid'] >= confid_watch_point:
                                False_Negative_times += 1
                            wrong_classification_output_image_folder_of_a_class = os.path.join(
                                FLAGS.output_wrong_classified_images_to_folder_path, 'wrong_classification_output_image_folder',
                                'ground_truth_of_{}'.format(inner_class_name), 'wrongly_to_{}'.format(target_class_name))
                            if not os.path.exists(wrong_classification_output_image_folder_of_a_class):
                                os.makedirs(wrong_classification_output_image_folder_of_a_class)
                            shutil.copy(wrong_info['file_full_path'],
                                        wrong_classification_output_image_folder_of_a_class)
                            

                    correctly_classified_times = len(
                        [confid for confid in stats['correctly_classified_confid_values'] if confid >= confid_watch_point])
                    print('     with confid_watch_point: {}'.format(confid_watch_point))
                    print('         Class: {} -> acc: {}({}/{}), recall: {}({}/{}+{}), avg infer(by_ms): {}, avg confid: {}, wrong infer: {{{}}}  \r'.format(
                        target_class_name.ljust(16),

                        str(correctly_classified_times /
                            int(stats['image_file_count']))[:5],
                        correctly_classified_times,
                        stats['image_file_count'],

                        str(correctly_classified_times /
                            (999999999 if correctly_classified_times + False_Negative_times == 0 else correctly_classified_times + False_Negative_times))[:5],
                        correctly_classified_times,
                        correctly_classified_times,
                        False_Negative_times,

                        str(1000*int(stats['total_infer_times_by_seconds']
                                     ) / int(stats['image_file_count']))[:7],
                        str(stats['total_infer_confid'] /
                            (999999999 if correctly_classified_times == 0 else correctly_classified_times))[:5],
                        ', '.join([i+"->"+str(len(stats['wrong_classified_to_classes_info'][i])) for i in stats['wrong_classified_to_classes_info'].keys()])))
                    print('\r')
            print('=====================================================================================================')
