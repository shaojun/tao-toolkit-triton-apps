import time
import unittest

from tao_triton.python.device_hub.utility.session_window import *


class TestSessionWindow(unittest.TestCase):

    def test_1_session_no_post_silent(self):
        session: list[str] = None

        def header_buffer_validation_predict(header_buffer: list[str]) -> bool:
            eb_count = 0
            for item in header_buffer:
                if item == "eb":
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            return eb_rate > 0.5

        def on_header_buffer_validated(header_buffer: list[str], is_header_buffer_valid: bool):
            # send block door msg to edge board
            self.assertTrue(is_header_buffer_valid)

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]):
            nonlocal session
            session = session_items

        sw: SessionWindow[str] = SessionWindow(
            lambda x: x == "eb", BufferType.ByPeriodTime, 2,
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
        self.assertEqual(sw.state, SessionState.SessionEnded)
        self.assertIsNotNone(session)
        self.assertEqual(session, ["eb", "people", "eb", "eb", "eb"])

        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.SessionEnded)
        time.sleep(3)
        sw.add("eb")
        sw.add("eb")
        self.assertEqual(sw.state, SessionState.SessionEnded)

    def test_2_sessions_no_post_silent(self):
        session: list[str] = None

        def header_buffer_validation_predict(header_buffer: list[str]) -> bool:
            eb_count = 0
            for item in header_buffer:
                if item == "eb":
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            return eb_rate > 0.5

        def on_header_buffer_validated(header_buffer: list[str], is_header_buffer_valid: bool):
            # send block door msg to edge board
            self.assertTrue(is_header_buffer_valid)

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]):
            nonlocal session
            session = session_items
            session_window.reset()
        sw: SessionWindow[str] = SessionWindow(
            lambda x: x == "eb", BufferType.ByPeriodTime, 2,
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
                         "eb", "eb", "eb", "doorsign", "eb", "eb", "eb",  "doorsign", "people", "eb"])

    def test_1_session_post_silent(self):
        session: list[str] = None

        def header_buffer_validation_predict(header_buffer: list[str]) -> bool:
            eb_count = 0
            for item in header_buffer:
                if item == "eb":
                    eb_count += 1
            eb_rate = eb_count / len(header_buffer)
            return eb_rate > 0.5

        def on_header_buffer_validated(header_buffer: list[str], is_header_buffer_valid: bool):
            # send block door msg to edge board
            self.assertTrue(is_header_buffer_valid)

        def on_session_end(session_window: SessionWindow[str], session_items: list[str]):
            nonlocal session
            session = session_items
            self.assertEqual(sw.state, SessionState.SessionEnded)
        sw: SessionWindow[str] = SessionWindow(
            lambda x: x == "eb", BufferType.ByPeriodTime, 2,
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


if __name__ == '__main__':
    unittest.main()
