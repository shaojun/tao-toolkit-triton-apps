import datetime
from enum import Enum
import shutil
from typing import List
import abc
from PIL import Image
import base64
import io
import time
import os
import uuid
from tao_triton.python.device_hub import util
from tao_triton.python.entrypoints import tao_client


class SessionType(int, Enum):
    # when an eb entered elevator, and finally left, this is a session, and ignored other factors like story etc.
    ElectricBicycleInElevatorSession = 1,

    # when one or more person entered elevator, and finally all of them are left, this is a session.
    PersonInElevatorSession = 2,

    DoorSignShowSession = 3,
    DorrSignHideSession = 4,


class SessionState(int, Enum):
    OPEN = 1,
    CLOSE = 2,


class SessionStateChangeListener:
    def on_state_change(self, session_id: str, new_state: SessionState):
        pass


class SessionBase:
    def __init__(self, session_type: SessionType):
        self.session_type = session_type
        self._state = SessionState.CLOSE
        self._open_time = datetime.datetime.now(datetime.timezone.utc)
        self._close_time = None
        self.state_change_listeners = []

    def regist_state_change_listener(self, listener: SessionStateChangeListener):
        self.state_change_listeners.append(listener)

    def unregist_state_change_listener(self, listener: SessionStateChangeListener):
        self.state_change_listeners.remove(listener)

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def open_time(self):
        return self._open_time

    @property
    def close_time(self):
        return self._close_time

    @abc.abstractmethod
    def feed(self, timeline_items: List):
        pass


class ElectricBicycleInElevatorSession(SessionBase):
    SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH = "ebic_image_samples"
    # avoid a single spike eb event mis-trigger alarm, set to 1 for disable the feature
    THROTTLE_Window_Depth = 2
    THROTTLE_Window_Time_By_Sec = 5

    def __init__(self, logging, timeline):
        super().__init__(SessionType.ElectricBicycleInElevatorSession)
        self.electric_bicycle_detector_name = "ElectricBicycleEnteringEventDetector"
        self.infer_server_ip_and_port = "127.0.0.1:8000"
        if util.read_config_fast_to_property(["developer_debug"], "enable_developer_local_debug_mode") == True:
            self.infer_server_ip_and_port = "36.153.41.18:18000"
        # the lib triton_client used for infer to remote triton server is based on a local image file, after the infer done, the file will be cleared.
        # this is the temp folder to store that temp image file
        self.temp_image_files_folder_name = "temp_infer_image_files"
        # 保存电车推理结果，电动车出梯时清除
        self.ebike_infer_result_list = []
        # 推理成功的结果
        self.infer_confirmed_eb_history_list = []
        self.silent_period = False
        self.latest_infer_success = None
        self.logger = logging.getLogger("electricBicycleInElevatorSessionLogger")
        self.statistics_logger = logging.getLogger("statisticsLogger")
        self.timeline = timeline

    def feed(self, timeline_items: List):
        # 1，从图片判定看图片是否是电车， 每张图片判定结果存放在list中
        # 2，电车进入时间，连续两张高识别率算进入？
        # 3，判定结果list清空条件，>5s认为电动车出梯，电车session结束
        # 4，对于静默期的定义，连续4（可配置）张低识别率的图片进入静默期，a,不再对session进行判定 b,detector需要取消阻门
        # 5，
        try:
            result = [i for i in timeline_items if
                      not i.consumed
                      and ("Vehicle|#|TwoWheeler" in i.raw_data and "|TwoWheeler|confirmed" not in i.raw_data)]
            # 没有目标识别的物体，如果有进行中的session需要判断是否需要结束
            if len(result) == 0:
                self.UpdateEbikeSession("", 0)
                return
            for item in result:
                packed_infer_result = self.inference_image_from_models(item)
                if packed_infer_result == None:
                    continue
                infered_class, infer_server_current_ebic_confid, edge_board_confidence, eb_image_base64_encode_text = packed_infer_result
                is_qua_board = item.version == "4.1qua"
                self.UpdateEbikeSession(infered_class, infer_server_current_ebic_confid, is_qua_board)
        except Exception as e:
            self.logger.exception("{} | {} | {}".format(
                self.timeline.board_id, "feed", "exception: {}".format(e)))

    def get_last_eb_image(self):
        pass

    # 推断当前的图片是否是电动车
    def inference_image_from_models(self, object_detection_timeline_item):
        """
        run 2 classification models on the ebic image, the first model is for 4 classes, and the 2nd model is for 2 classe
        @param object_detection_timeline_item: TimelineItem that has ebic image data which send from edge board
        @param current_story: current story of the elevator, for now only used for logging and statistics
        @return:
        """
        is_qua_board = object_detection_timeline_item.version == "4.1qua"

        infer_start_time = time.time()
        sections = object_detection_timeline_item.raw_data.split('|')

        # the last but one is the detected and cropped object image file with base64 encoded text,
        # and the section is prefixed with-> base64_image_data:
        cropped_base64_image_file_text = sections[len(sections) - 2][len("base64_image_data:"):]

        # the last but two is the OPTIONAL, full image file with base64 encoded text,
        # and the section is prefixed with-> full_base64_image_data:
        # upload the full image to cloud is for debug purpose, it's controlled and cofigurable from edge board local side.
        full_image_frame_base64_encode_text = sections[len(
            sections) - 3][len("full_base64_image_data:"):]

        edge_board_confidence_str: str = sections[len(sections) - 1]
        edge_board_confidence = float(str(edge_board_confidence_str)[:4])

        temp_cropped_image_file_full_name = os.path.join(self.temp_image_files_folder_name,
                                                         str(uuid.uuid4()) + '.jpg')
        temp_image = Image.open(io.BytesIO(base64.decodebytes(
            cropped_base64_image_file_text.encode('ascii'))))
        temp_image.save(temp_cropped_image_file_full_name)

        if is_qua_board and util.read_config_fast_to_property(["detectors",
                                                               self.electric_bicycle_detector_name],
                                                              'enable_infer_for_kua_board') == False:
            self.save_sample_image(temp_cropped_image_file_full_name, object_detection_timeline_item.original_timestamp,
                                   "electric_bicycle", edge_board_confidence, full_image_frame_base64_encode_text)
            self.logger.debug("qua board id:{}, infer class electric_bicycle, board confidence:{}".format(
                self.timeline.board_id,
                edge_board_confidence / 100
            ))
            return "electric_bicycle", edge_board_confidence / 100, edge_board_confidence / 100, full_image_frame_base64_encode_text

        # only used when 2nd model declined the eb detect from 1st model, and when decling,
        # the part of 2nd model logic should have done save the sample image.
        # the next save sample image should not be called anymore.
        this_sample_image_already_saved = False
        try:
            # self.statistics_logger.debug("{} | {} | {}".format(self.timeline.board_id, "1st_model_pre_infer",""))
            raw_infer_results = tao_client.callable_main(['-m', 'elenet_four_classes_230722_tao',
                                                          '--mode', 'Classification',
                                                          '-u', self.infer_server_ip_and_port,
                                                          '--output_path', './',
                                                          temp_cropped_image_file_full_name])
            infered_class = raw_infer_results[0][0]['infer_class_name']
            infer_server_current_ebic_confid = raw_infer_results[0][0]['infer_confid']
            self.statistics_logger.debug("{} | {} | {}".format(
                self.timeline.board_id,
                "1st_model_post_infer",
                "infered_class: {}, infered_confid: {}, used_time: {}".format(
                    infered_class,
                    infer_server_current_ebic_confid,
                    str(time.time() - infer_start_time)[:5])))
        except Exception as e:
            self.statistics_logger.exception("{} | {} | {}".format(
                self.timeline.board_id,
                "1st_model_post_infer",
                "exception: {}".format(e)
            ))
            self.logger.exception(
                "tao_client.callable_main(with model: elenet_four_classes_230722_tao) rasised an exception: {}".format(
                    e))
            return

        try:
            if util.read_config_fast_to_property(
                    ["detectors",
                     self.electric_bicycle_detector_name,
                     'second_infer'],
                    'enable') \
                    and infered_class == 'electric_bicycle' \
                    and infer_server_current_ebic_confid >= \
                    util.read_config_fast_to_property(["detectors",
                                                       self.electric_bicycle_detector_name],
                                                      'ebic_confid') \
                    and infer_server_current_ebic_confid < \
                    util.read_config_fast_to_property(["detectors",
                                                       self.electric_bicycle_detector_name,
                                                       'second_infer'],
                                                      'bypass_if_previous_model_eb_confid_greater_or_equal_than'):
                second_infer_raw_infer_results = tao_client.callable_main(['-m', 'elenet_two_classes_240602_tao',
                                                                           '--mode', 'Classification',
                                                                           '-u', self.infer_server_ip_and_port,
                                                                           '--output_path', './',
                                                                           temp_cropped_image_file_full_name])
                # classes = ['eb', 'non_eb']
                second_infer_infered_class = second_infer_raw_infer_results[0][0]['infer_class_name']
                second_infer_infered_confid = second_infer_raw_infer_results[0][0]['infer_confid']
                self.statistics_logger.debug("{} | {} | {}".format(
                    self.timeline.board_id,
                    "2nd_model_post_infer",
                    "infered_confid: {}, infered_confid: {}".format(
                        second_infer_infered_class, second_infer_infered_confid)
                ))
                if second_infer_infered_class == 'eb':
                    self.statistics_logger.debug("{} | {} | {}".format(
                        self.timeline.board_id,
                        "2nd_model_post_infer_confirm_eb",
                        "eb_confid: {}".format(
                            second_infer_infered_confid)
                    ))
                    pass
                else:
                    non_eb_threshold = util.read_config_fast_to_property(["detectors",
                                                                          self.electric_bicycle_detector_name,
                                                                          'second_infer'],
                                                                         'still_treat_as_eb_if_non_eb_confid_less_or_equal_than')
                    non_eb_confid = second_infer_raw_infer_results[0][0]['infer_confid']

                    # though 2nd model say it's non-eb, but for avoid too further hit down the accuracy, we still treat it
                    # if the non-eb confid is less than the threshold
                    if non_eb_confid <= non_eb_threshold:
                        self.statistics_logger.debug("{} | {} | {}".format(
                            self.timeline.board_id,
                            "2nd_model_post_infer_confirm_eb",
                            "non_eb_confid: {}".format(non_eb_confid)
                        ))
                        pass
                    else:
                        self.statistics_logger.debug("{} | {} | {}".format(
                            self.timeline.board_id,
                            "2nd_model_post_infer_sink_eb",
                            "non_eb_confid: {}".format(non_eb_confid)
                        ))
                        self.logger.debug(
                            "      board: {}, sink this eb(from 4 class model) due to detect as non_eb(from 2 class model) with confid: {}"
                            .format(
                                self.timeline.board_id,
                                non_eb_confid))
                        this_sample_image_already_saved = True
                        second_model_declined__sample_image_file_name_prefix = "2nd_m_non_eb_{}___".format(
                            str(non_eb_confid)[:4])
                        self.save_sample_image(temp_cropped_image_file_full_name,
                                               object_detection_timeline_item.original_timestamp,
                                               infered_class, infer_server_current_ebic_confid,
                                               full_image_frame_base64_encode_text,
                                               second_model_declined__sample_image_file_name_prefix)
                        infered_class = 'other_unknown_stuff'
                        infer_server_current_ebic_confid = non_eb_confid
        except Exception as e:
            self.statistics_logger.exception("{} | {} | {}".format(
                self.timeline.board_id,
                "2nd_model_post_infer",
                "exception: {}".format(e)
            ))
            self.logger.exception(
                "tao_client.callable_main(with 2nd model: elenet_two_classes_240413_tao) rasised an exception: {}".format(
                    e))
            return

        t1 = time.time()

        infer_used_time = (t1 - infer_start_time) * 1000
        self.logger.debug(
            "      board: {}, time used for infer:{}ms(localConf:{}), raw infer_results/confid: {}/{}".format(
                self.timeline.board_id, str(infer_used_time)[:5],
                edge_board_confidence, infered_class, infer_server_current_ebic_confid))

        # if infered_class == 'electric_bicycle':
        try:
            if this_sample_image_already_saved == False:
                self.save_sample_image(temp_cropped_image_file_full_name,
                                       object_detection_timeline_item.original_timestamp,
                                       infered_class, infer_server_current_ebic_confid,
                                       full_image_frame_base64_encode_text)
        except:
            self.logger.exception(
                "save_sample_image(...) rasised an exception:")

        if os.path.isfile(temp_cropped_image_file_full_name) or os.path.islink(temp_cropped_image_file_full_name):
            os.unlink(temp_cropped_image_file_full_name)

        return infered_class, infer_server_current_ebic_confid, edge_board_confidence, full_image_frame_base64_encode_text

    # 保存图片
    def save_sample_image(self, image_file_full_name, original_utc_timestamp: datetime, infered_class,
                          infer_server_ebic_confid, full_base64_image_file_text, dst_file_name_prefix=""):
        image_sample_path = os.path.join(
            ElectricBicycleInElevatorSession.SAVE_EBIC_IMAGE_SAMPLE_ROOT_FOLDER_PATH, self.timeline.board_id)
        if not os.path.exists(image_sample_path):
            os.makedirs(image_sample_path)
        board_original_zone8_timestamp_str = str(original_utc_timestamp.astimezone(
            datetime.datetime.now().tzinfo).strftime("%Y_%m%d_%H%M_%S_%f")[:-3])
        dh_local_timestamp_str = str(datetime.datetime.now().strftime("%H%M_%S_%f")[:-3])
        file_name_prefix = ''
        if infered_class == 'electric_bicycle':
            pass
        else:
            file_name_prefix = infered_class + "_"
        file_name_prefix = dst_file_name_prefix + file_name_prefix
        shutil.copyfile(image_file_full_name,
                        os.path.join(
                            image_sample_path,
                            file_name_prefix + str(infer_server_ebic_confid)[
                                               :4] + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + "___" + self.timeline.board_id + ".jpg"))
        if full_base64_image_file_text and len(full_base64_image_file_text) > 1:
            temp_full_image = Image.open(io.BytesIO(base64.decodebytes(full_base64_image_file_text.encode('ascii'))))
            temp_full_image.save(os.path.join(image_sample_path,
                                              str(infer_server_ebic_confid) + "___full_image__" + self.timeline.board_id + "___" + board_original_zone8_timestamp_str + "___" + dh_local_timestamp_str + ".jpg"))

    # avoid a single spike eb event mis-trigger alarm, set to 1 for disable the feature
    # we noticed some cases that like a bicycle entering, most of the frames can be infered as bicycle which is correct,
    # but sometimes, a Single frame can be infered as eb, which is wrong, here is trying to avoid this case
    def CheckIfThrottleNeeded(self, confirmed_confid, is_qua_board=False):
        if is_qua_board and util.read_config_fast_to_property(["detectors",
                                                               self.electric_bicycle_detector_name],
                                                              'enable_throttle_for_kua_board') == False:
            return False

        self.infer_confirmed_eb_history_list.append({"infer_time": datetime.datetime.now()})
        # remove multiple expired items at once from a list
        expired_items = [i for i in self.infer_confirmed_eb_history_list if
                         (datetime.datetime.now() - i[
                             "infer_time"]).total_seconds() > ElectricBicycleInElevatorSession.THROTTLE_Window_Time_By_Sec]
        self.infer_confirmed_eb_history_list = [
            i for i in self.infer_confirmed_eb_history_list if i not in expired_items]
        # since the infer is so sure, then let it go through and not to throttle.
        if confirmed_confid >= 0.99:
            return False
        if len(self.infer_confirmed_eb_history_list) < ElectricBicycleInElevatorSession.THROTTLE_Window_Depth:
            return True
        return False

    # 1，推理是电动车，且推理出结果高于配置值ebic_confid
    # 2. 非电车或者可信度低 两种情况：
    # a 已处于入梯状态 非电车判断是否符合出梯条件
    # b 电动车还未入梯 考虑是否进入静默期
    # 如果没有电车图片上传，infer_class传空字符串， infer_server_current_ebic_confid传0用来判断session是否结束
    def UpdateEbikeSession(self, infer_class, infer_server_current_ebic_confid, is_qua_board=False):
        # self.logger.debug("board:{}, in UpdateEbikeSession, infer_class:{}, confid:{}".format(
        #    self.timeline.board_id, infer_class, infer_server_current_ebic_confid
        # ))
        if infer_class == "":
            if len(self.ebike_infer_result_list) > 0:
                time_treat_exit = util.read_config_fast_to_property(["detectors", self.electric_bicycle_detector_name],
                                                                    'how_long_to_treat_eb_exit_when_no_image_uploded')
                surrived_item = [i for i in self.ebike_infer_result_list if
                                 (datetime.datetime.now() - i["time_stamp"]).total_seconds() < time_treat_exit]
                if not (len(surrived_item) > 0):
                    if self._state == SessionState.OPEN:
                        self._state = SessionState.CLOSE
                        self._close_time = datetime.datetime.now()
                        self.logger.debug(
                            "board:{} close the ebik session due to long time no ebik object received".format(
                                self.timeline.board_id))
                    self.ebike_infer_result_list = []
            return
        ebic_confid = util.read_config_fast_to_property(["detectors", self.electric_bicycle_detector_name],
                                                        'ebic_confid')

        self.ebike_infer_result_list.append({"infer_class": infer_class,
                                             "confid": infer_server_current_ebic_confid,
                                             "time_stamp": datetime.datetime.now()})
        # 检查是否处于静默期，如果在静默期内，不做session处理
        if self.CheckIfInSlientDuration(ebic_confid, is_qua_board):
            if self.state == SessionState.OPEN:
                self._state = SessionState.CLOSE
                self._close_time = datetime.datetime.now()
            self.logger.debug("board:{} sink the image due to the slient duration".format(self.timeline.board_id))
            return

        time_to_keep_successful_infer_result = util.read_config_fast_to_property(
            ["detectors", self.electric_bicycle_detector_name],
            'time_to_keep_successful_infer_result')
        keep_ebike_confid_threshold = util.read_config_fast_to_property(
            ["detectors", self.electric_bicycle_detector_name], 'keep_ebic_confid')

        if infer_class == "electric_bicycle":
            if infer_server_current_ebic_confid > ebic_confid:
                # 推理结果为电动车，分两种情况 1，电动车还未入梯，需检车是否符合入梯条件 2，已经入梯了，只需更新最近一次成功推理的时间
                self.latest_infer_success = datetime.datetime.now()
                if self._state == SessionState.CLOSE:
                    if not self.CheckIfThrottleNeeded(infer_server_current_ebic_confid, is_qua_board):
                        self._state = SessionState.OPEN
                        self._open_time = datetime.datetime.now()
                        self.logger.debug(
                            "board:{},session opened, infer_class:{}, ebic_confid:{}".format(self.timeline.board_id,
                                                                                             infer_class,
                                                                                             infer_server_current_ebic_confid))
            else:
                # 推理结果小于配置的阈值 1，已处理电车入梯的状态 1)配置了保留电动车入梯状态的阈值 2）没有配置
                #  2，电车还未入梯
                if self._state == SessionState.OPEN:
                    # 继续保留成功推理
                    if keep_ebike_confid_threshold != 0 and infer_server_current_ebic_confid > keep_ebike_confid_threshold:
                        self.latest_infer_success = datetime.datetime.now()
                        self.logger.debug("board:{},keep session opened, infer_class:{}, ebic_confid:{}".format(
                            self.timeline.board_id,
                            infer_class,
                            infer_server_current_ebic_confid))
                    elif self.latest_infer_success != None and (
                            datetime.datetime.now() - self.latest_infer_success).total_seconds() > time_to_keep_successful_infer_result:
                        # 距离上一次成功推理为电动车已经过去一段时间，可以认为电车出梯
                        self._state = SessionState.CLOSE
                        self._close_time = datetime.datetime.now()
                        self.latest_infer_success = None
                        self.ebike_infer_result_list = []
                        self.logger.debug(
                            "board:{},close the open session, infer_class:{}, latest success result at:{}".format(
                                self.timeline.board_id, infer_class, self.latest_infer_success
                            ))
        else:
            # 推理结果为非电动车, 但当前已经处于电动车入梯的状态 1, 距最后一次推理成功已经超过配置的时间
            if self._state == SessionState.OPEN and self.latest_infer_success != None and \
                    (
                            datetime.datetime.now() - self.latest_infer_success).total_seconds() > time_to_keep_successful_infer_result:
                self._state = SessionState.CLOSE
                self._close_time = datetime.datetime.now()
                self.latest_infer_success = None
                self.ebike_infer_result_list = []
                self.logger.debug("board:{},close the open session, infer_class:{}, latest success result at:{}".format(
                    self.timeline.board_id, infer_class, self.latest_infer_success))
            pass

    # 是否处于静默期
    # 1，一轮图片的前n张都是非电车或者识别出的可信都都低于配置的阈值
    # 2，如果第一张图片的时间已经超过了静默时间，则静默结束
    # 问题：静默期结束是否需要清空inferlist??
    def CheckIfInSlientDuration(self, ebic_confid, is_qua_board=False):
        if is_qua_board and util.read_config_fast_to_property(["detectors",
                                                               self.electric_bicycle_detector_name],
                                                              'enable_silent_period_for_kua_board') == False:
            self.silent_period = False
            return False
        max_infer_count = util.read_config_fast_to_property(["detectors", self.electric_bicycle_detector_name],
                                                            'infering_stage__how_many_continuous_non_eb_see_then_enter_silent_period')

        infering_stage__when_see_many_non_eb_then_enter_silent_period_duration = util.read_config_fast_to_property(
            ["detectors", self.electric_bicycle_detector_name],
            'infering_stage__when_see_many_non_eb_then_enter_silent_period_duration')
        # 仍在静默期内
        if self.silent_period and self.silent_start_at != None and (datetime.datetime.now() -
                                                                    self.silent_start_at).total_seconds() < infering_stage__when_see_many_non_eb_then_enter_silent_period_duration:
            return True

        # 静默时间超过配置值，静默期结束
        if self.silent_period and self.silent_start_at != None and (datetime.datetime.now() -
                                                                    self.silent_start_at).total_seconds() > infering_stage__when_see_many_non_eb_then_enter_silent_period_duration:
            self.silent_period = False
            if len(self.ebike_infer_result_list) > max_infer_count:
                self.ebike_infer_result_list = []
            self.silent_start_at = None
            self.logger.debug("board:{}, slient duration end".format(self.timeline.board_id))
            return False

        if len(self.ebike_infer_result_list) < max_infer_count:
            self.silent_period = False
            self.silent_start_at = None
        else:
            surrived_items = self.ebike_infer_result_list[0:max_infer_count]
            surrived_items = [i for i in surrived_items if i["confid"] >= ebic_confid and
                              i["infer_class"] == "electric_bicycle"]
            # 如果连续前n张图片识别出的可信度都小于配置的阈值那么需要进入静默期
            if len(surrived_items) == 0 and (datetime.datetime.now() -
                                             self.ebike_infer_result_list[0][
                                                 "time_stamp"]).total_seconds() < infering_stage__when_see_many_non_eb_then_enter_silent_period_duration:
                if self.silent_period == False:
                    self.silent_period = True
                    self.silent_start_at = datetime.datetime.now()
                    self.logger.debug("board:{}, slient duration start".format(self.timeline.board_id))
            elif len(surrived_items) == 0 and (datetime.datetime.now() -
                                               self.ebike_infer_result_list[0][
                                                   "time_stamp"]).total_seconds() >= infering_stage__when_see_many_non_eb_then_enter_silent_period_duration:
                self.silent_period = False
                self.ebike_infer_result_list = []
                self.silent_start_at = None
                self.logger.debug("board:{}, slient duration end".format(self.timeline.board_id))
        return self.silent_period


class PersonInElevatorSession(SessionBase):
    def __init__(self):
        super().__init__(SessionType.PersonInElevatorSession)

    def feed(self, timeline_items: List):
        pass

    def get_last_person_image(self):
        pass


class SessionManager:
    def __init__(self):
        self.sessions = {ElectricBicycleInElevatorSession()}
        self.history = {}
        for session in self.sessions:
            self.history = {session: []}

    def feed(self, timeline_items: List):
        for session in self.sessions:
            session.feed(timeline_items)
            if session.state == SessionState.CLOSE:
                self.history[session].append(session)
                # only keep last 3 sessions
                if len(self.history[session]) > 3:
                    self.history[session].pop(0)
