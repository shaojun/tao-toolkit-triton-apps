import time
import unittest
import sys
import os
from tao_triton.python.device_hub.utility.session_window import *


class TestSessionWindow(unittest.TestCase):

    def test_1_session_no_post_silent(self):
        session: list[str] = None

        def header_buffer_validation_predict(header_buffer: list[str]) -> tuple[bool, any]:
            eb_count = 0
            for item in header_buffer:
                if item == "eb":
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            return eb_rate > 0.5, "i am the reason str"

        def on_header_buffer_validated(header_buffer: list[str], predict_data: any, is_header_buffer_valid: bool):
            # send block door msg to edge board
            self.assertTrue(is_header_buffer_valid)
            self.assertEqual(predict_data, "i am the reason str")

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]):
            nonlocal session
            session = session_items

        sw: SessionWindow[str] = SessionWindow(
            lambda x: x == "eb", None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, on_header_buffer_validated, 0,
            lambda new_item, items: True, BufferType.ByPeriodTime, 1,
            on_session_end=on_session_end)
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add("people")
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add("people")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        time.sleep(2.3)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        sw.add("eb")
        time.sleep(1.3)
        self.assertEqual(sw.state, SessionState.SessionEnd)
        self.assertIsNotNone(session)
        self.assertEqual(session, ["eb", "people", "eb", "eb", "eb"])

        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.SessionEnd)
        time.sleep(3)
        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.SessionEnd)

    def test_2_sessions_no_post_silent(self):
        session: list[str] = None

        def header_buffer_validation_predict(header_buffer: list[str]) -> tuple[bool, any]:
            eb_count = 0
            for item in header_buffer:
                if item == "eb":
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            return eb_rate > 0.5, "i am the reason str"

        def on_header_buffer_validated(header_buffer: list[str], predict_data: any, is_header_buffer_valid: bool):
            # send block door msg to edge board
            self.assertTrue(is_header_buffer_valid)
            self.assertEqual(predict_data, "i am the reason str")

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]):
            nonlocal session
            session = session_items
            session_window.reset()

        sw: SessionWindow[str] = SessionWindow(
            lambda x: x == "eb", None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, on_header_buffer_validated, 0,
            lambda new_item, items: True, BufferType.ByPeriodTime, 1,
            on_session_end=on_session_end)
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add("people")
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add("people")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        time.sleep(2.3)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        sw.add("eb")
        time.sleep(1.3)
        self.assertIsNotNone(session)
        self.assertEqual(session, ["eb", "people", "eb", "eb", "eb"])

        session = None
        sw.add("eb")
        sw.add("eb")
        sw.add("eb")
        sw.add("doorsign")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        time.sleep(2.3)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        sw.add("eb")
        sw.add("eb")
        sw.add("eb")
        sw.add("doorsign")
        sw.add("people")
        sw.add("eb")
        time.sleep(1.3)
        self.assertIsNotNone(session)
        self.assertEqual(session, [
            "eb", "eb", "eb", "doorsign", "eb", "eb", "eb", "doorsign", "people", "eb"])

    def test_1_session_post_silent(self):
        session: list[str] = None

        def header_buffer_validation_predict(header_buffer: list[str]) -> tuple[bool, any]:
            eb_count = 0
            for item in header_buffer:
                if item == "eb":
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            return eb_rate > 0.5, "i am the reason str"

        def on_header_buffer_validated(header_buffer: list[str], predict_data: any, is_header_buffer_valid: bool):
            # send block door msg to edge board
            self.assertTrue(is_header_buffer_valid)
            self.assertEqual(predict_data, "i am the reason str")

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]):
            nonlocal session
            session = session_items
            self.assertEqual(sw.state, SessionState.SessionEnd)

        sw: SessionWindow[str] = SessionWindow(
            lambda x: x == "eb", None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, on_header_buffer_validated, 0,
            lambda new_item, items: True, BufferType.ByPeriodTime, 1,
            on_session_end=on_session_end,
            post_session_silent_time=2)
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add("people")
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add("people")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        time.sleep(2.3)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        sw.add("eb")
        time.sleep(1.3)
        self.assertEqual(sw.state, SessionState.InPostSilentTime)
        self.assertIsNotNone(session)
        self.assertEqual(session, ["eb", "people", "eb", "eb", "eb"])

        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.InPostSilentTime)
        time.sleep(2.2)
        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.InPostSilentTime)

    def test_gas_tank_entring(self):
        # 煤气罐入梯条件1，图片推理为煤气罐，可信度大于0.9 2，张数暂定1张
        # 3，煤气罐入梯后30s认为session结束 4，结束后进入silent时间
        def header_buffer_starter_validation(item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            gas_tank_count = 0
            for item in header_buffer:
                if item["class"] == "gastank" and item["confid"] > 0.9:
                    gas_tank_count += 1
            return gas_tank_count / len(header_buffer) > 0.5, "i am the reason str"

        def on_header_buffer_validated(buffer: list[dict], predict_data: any, is_header_buffer_valid: bool) -> None:
            # 进入body_buffering状态
            self.assertTrue(is_header_buffer_valid)
            self.assertEqual(predict_data, "i am the reason str")

        def body_buffer_validation(items: list[dict], item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]) -> None:
            # self.assertEqual(sw.state, SessionState.SessionEnd)
            # session 结束，结束告警
            pass

        def on_post_session_silent_time_elapsed(items: list[dict]):
            sw.reset()

        sw: SessionWindow[dict] = SessionWindow(
            header_buffer_starter_validation, None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, on_header_buffer_validated, 0,
            body_buffer_validation, BufferType.ByPeriodTime, 5,
            on_session_end=on_session_end,
            post_session_silent_time=5,
            on_post_session_silent_time_elapsed=on_post_session_silent_time_elapsed)

        sw.add({"class": "gastank", "confid": 0.8})
        self.assertEqual(sw.state, SessionState.Uninitialized)

        sw.add({"class": "gastank", "confid": 0.91})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)

        sw.add({"class": "gastank", "confid": 0.91})
        time.sleep(2.5)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        time.sleep(5)
        self.assertEqual(sw.state, SessionState.InPostSilentTime)
        time.sleep(3)
        self.assertEqual(sw.state, SessionState.InPostSilentTime)
        sw.add({"class": "gastank", "confid": 0.91})
        sw.add({"class": "gastank", "confid": 0.91})
        # 第一条不满足header条件，所以不会开启session
        self.assertEqual(len(sw.items), 2)
        time.sleep(3)
        self.assertEqual(sw.state, SessionState.Uninitialized)

    def test_new_session_started_after_post_silient(self):
        def header_buffer_starter_validation(item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            gas_tank_count = 0
            for item in header_buffer:
                if item["class"] == "gastank" and item["confid"] > 0.9:
                    gas_tank_count += 1
            return gas_tank_count / len(header_buffer) > 0.5, "i am the reason str"

        def on_header_buffer_validated(buffer: list[dict], predict_data: any, is_header_buffer_valid: bool) -> None:
            # 进入body_buffering状态
            self.assertTrue(is_header_buffer_valid)
            self.assertEqual(predict_data, "i am the reason str")

        def body_buffer_validation(items: list[dict], item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]) -> None:
            # self.assertEqual(sw.state, SessionState.SessionEnd)
            # # session 结束，结束告警
            pass

        def on_post_session_silent_time_elapsed(items: list[dict]):
            sw.reset()

        sw: SessionWindow[dict] = SessionWindow(
            header_buffer_starter_validation, None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, on_header_buffer_validated, 0,
            body_buffer_validation, BufferType.ByPeriodTime, 5,
            on_session_end=on_session_end,
            post_session_silent_time=5,
            on_post_session_silent_time_elapsed=on_post_session_silent_time_elapsed)

        sw.add({"class": "gastank", "confid": 0.92})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add({"class": "gastank", "confid": 0.91})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add({"class": "gastank", "confid": 0.91})
        sw.add({"class": "others", "confid": 0.91})
        sw.add({"class": "others", "confid": 0.91})
        time.sleep(2.5)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        sw.add({"class": "others", "confid": 0.91})
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        time.sleep(1)
        sw.add({"class": "gastank", "confid": 0.91})
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        time.sleep(4)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        time.sleep(2)
        self.assertEqual(sw.state, SessionState.InPostSilentTime)
        time.sleep(3)
        self.assertEqual(sw.state, SessionState.InPostSilentTime)
        sw.add({"class": "gastank", "confid": 0.91})
        sw.add({"class": "gastank", "confid": 0.91})
        # 进入post slient time之前所有图片应该都在list内,去掉一个不满足body buffering条件的item
        self.assertEqual(len(sw.items), 6)
        time.sleep(3)
        self.assertEqual(sw.state, SessionState.Uninitialized)

        # post silent结束，进入新的session
        self.assertEqual(len(sw.items), 0)
        sw.add({"class": "gastank", "confid": 0.91})
        self.assertEqual(len(sw.items), 1)
        self.assertEqual(sw.state, SessionState.HeaderBuffering)

    def test_pre_silent_period(self):
        # 测试header buffer不满足条件情况下，进入pre silent time
        def header_buffer_starter_validation(item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            gas_tank_count = 0
            for item in header_buffer:
                if item["class"] == "gastank" and item["confid"] > 0.9:
                    gas_tank_count += 1
            return gas_tank_count / len(header_buffer) > 0.5, "i am the reason str"

        sw: SessionWindow[dict] = SessionWindow(
            header_buffer_starter_validation, None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, None, 6,
            None, BufferType.ByPeriodTime, 5)

        sw.add({"class": "gastank", "confid": 0.91})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add({"class": "gastank", "confid": 0.6})
        sw.add({"class": "gastank", "confid": 0.7})
        sw.add({"class": "gastank", "confid": 0.8})
        sw.add({"class": "gastank", "confid": 0.9})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        time.sleep(2.2)
        self.assertEqual(sw.state, SessionState.InPreSilentTime)
        self.assertEqual(len(sw.items), 5)
        sw.add({"class": "gastank", "confid": 0.92})
        self.assertEqual(len(sw.items), 5)
        time.sleep(6.2)
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add({"class": "gastank", "confid": 0.92})
        self.assertEqual(len(sw.items), 1)
        self.assertEqual(sw.state, SessionState.HeaderBuffering)

    def test_pass_predict_data_to_on_header_buffer_validated(self):
        # 测试header buffer不满足条件情况下，进入pre silent time
        def header_buffer_starter_validation(item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            gas_tank_count = 0
            for item in header_buffer:
                if item["class"] == "gastank" and item["confid"] > 0.9:
                    gas_tank_count += 1
            return gas_tank_count / len(header_buffer) > 0.5, "i am the reason str"

        def on_header_buffer_validated(buffer: list[dict], predict_data: any, is_header_buffer_valid: bool) -> None:
            # 进入body_buffering状态
            self.assertTrue(predict_data == "i am the reason str")

        sw: SessionWindow[dict] = SessionWindow(
            header_buffer_starter_validation, None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, on_header_buffer_validated, 6,
            None, BufferType.ByPeriodTime, 5)

        sw.add({"class": "gastank", "confid": 0.91})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        sw.add({"class": "gastank", "confid": 0.6})
        sw.add({"class": "gastank", "confid": 0.7})
        sw.add({"class": "gastank", "confid": 0.8})
        sw.add({"class": "gastank", "confid": 0.9})
        self.assertEqual(sw.state, SessionState.HeaderBuffering)
        time.sleep(2.2)
        self.assertEqual(sw.state, SessionState.InPreSilentTime)
        self.assertEqual(len(sw.items), 5)
        sw.add({"class": "gastank", "confid": 0.92})
        self.assertEqual(len(sw.items), 5)
        time.sleep(6.5)
        self.assertEqual(sw.state, SessionState.Uninitialized)
        sw.add({"class": "gastank", "confid": 0.92})
        self.assertEqual(len(sw.items), 1)
        self.assertEqual(sw.state, SessionState.HeaderBuffering)

    def test_no_item_after_header_buffer_valid(self):
        # session 开启后再也没有item放入，session能否自动结束
        # 测试header buffer不满足条件情况下，进入pre silent time
        def header_buffer_starter_validation(item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def header_buffer_validation_predict(header_buffer: list[dict]) -> tuple[bool, any]:
            gas_tank_count = 0
            for item in header_buffer:
                if item["class"] == "gastank" and item["confid"] > 0.9:
                    gas_tank_count += 1
            return gas_tank_count / len(header_buffer) > 0.5, "i am the reason str"

        def body_buffer_validation(items: list[dict], item: dict) -> bool:
            return item["class"] == "gastank" and item["confid"] > 0.9

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]) -> None:
            pass

        sw: SessionWindow[dict] = SessionWindow(
            header_buffer_starter_validation, None, BufferType.ByPeriodTime, 2,
            header_buffer_validation_predict, None, 10,
            body_buffer_validation, BufferType.ByPeriodTime, 5, on_session_end=on_session_end)

        sw.add({"class": "gastank", "confid": 0.92})
        sw.add({"class": "gastank", "confid": 0.92})
        time.sleep(2)
        self.assertEqual(sw.state, SessionState.BodyBuffering)
        time.sleep(5.5)
        self.assertEqual(sw.state, SessionState.SessionEnd)


if __name__ == '__main__':
    unittest.main()
