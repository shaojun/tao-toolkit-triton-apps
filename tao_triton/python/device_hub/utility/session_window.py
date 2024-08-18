from __future__ import annotations
from enum import Enum
import threading
from typing import Callable, TypeVar, Generic, Optional
T = TypeVar("T")


class SessionState(Enum):
    Uninitialized = 1
    """
    InPreSilentTime: entered this state when header buffer validated with false, during this time, add(...) will be ignored
    """
    InPreSilentTime = 2
    HeaderBuffering = 3
    BodyBuffering = 4
    SessionEnded = 5
    """
    InPostSilentTime: entered this state when session ended, during this time, add(...) will be ignored
    """
    InPostSilentTime = 6


class BufferType(Enum):
    ByItemCount = 1,
    ByPeriodTime = 2


class SessionWindow(Generic[T]):
    def __init__(self,
                 header_buffer_starter_predict:  Callable[[T], bool],
                 header_buffer_type: BufferType,
                 header_buffer_end_condition: int,
                 header_buffer_validation_predict: Callable[[list[T]], bool],
                 on_header_buffer_validated: Optional[Callable[[list[T], bool], None]],
                 pre_session_silent_time: int,

                 body_buffer_validation_predict: Callable[[list[T], T], bool],
                 body_buffer_type: BufferType,                 
                 body_buffer_end_condition: int,

                 on_session_end: Callable[[any, list[T]], None] = None,

                 post_session_silent_time: int = 0,
                 on_post_session_silent_time_elapsed: Optional[Callable[[
                     list[T]], None]] = None,
                 ):
        """
        @param header_buffer_starter_predict: a function to predict if the item is the starter of a header buffer
        @param header_buffer_type: the type of header buffer, by item count or by fixed time
        @param header_buffer_condition: the condition to trigger header buffer validation
        @param header_buffer_validation_predict: a function to validate the header buffer
        @param on_header_buffer_validated: a callback function when header buffer validated, if validated with true, a session will be started
        @param pre_session_silent_time: the time of silent when on_header_buffer_validated with false, during this time, add(...) will be ignored
        @param body_buffer_type: the type of body buffer, by item count or by fixed time
        @param body_buffer_validation_predict: a function to validate the body buffer
        @param body_buffer_end_condition: the condition to trigger session end
        @param on_session_end: a callback function when session end
        @param post_session_silent_time: the time of silent after session end
        @param on_post_session_silent_time_elapsed: a callback function when post session silent time elapsed
        """
        self.header_buffer_starter_predict = header_buffer_starter_predict
        self.header_buffer_type = header_buffer_type
        self.header_buffer_condition = header_buffer_end_condition
        self.header_buffer_validation_predict = header_buffer_validation_predict
        self.on_header_buffer_validated = on_header_buffer_validated
        self.pre_session_silent_time = pre_session_silent_time

        self.body_buffer_validation_predict = body_buffer_validation_predict

        self.session_end_with_fixed_time_from_last_item = body_buffer_end_condition
        self.on_session_end = on_session_end
        self.session_truncate_timer = None

        self.post_session_silent_time = post_session_silent_time
        self.on_post_session_silent_time_elapsed = on_post_session_silent_time_elapsed
        self.silent_timer = None

        self.items: list[T] = []
        self.state = SessionState.Uninitialized

        self.lock = threading.Lock()

    def reset(self):
        self.items = []
        self.state = SessionState.Uninitialized

    def __on_session_truncated__(self):
        self.session_truncate_timer.cancel()
        with self.lock:
            if self.state != SessionState.SessionEnded:
                self.state = SessionState.SessionEnded
                self.on_session_end(self, self.items)
                # if not self.silent_time_timeout:
                #     self.reset()
                #     return
                if self.post_session_silent_time > 0:
                    self.state = SessionState.InPostSilentTime

                    def on_silent_time_timedout():
                        self.silent_timer.cancel()
                        if self.on_post_session_silent_time_elapsed:
                            self.on_post_session_silent_time_elapsed(
                                self.items)
                        # self.reset()
                    self.silent_timer = threading.Timer(self.post_session_silent_time,
                                                        on_silent_time_timedout)
                    self.silent_timer.start()

    def add(self, item: T):
        if self.state == SessionState.Uninitialized:
            if self.header_buffer_starter_predict(item):
                self.state = SessionState.HeaderBuffering
            else:
                if self.pre_session_silent_time and self.pre_session_silent_time > 0:
                    self.state = SessionState.InPreSilentTime
                return

        if self.state == SessionState.HeaderBuffering:
            self.items.append(item)
            if self.header_buffer_type == BufferType.ByItemCount:
                if len(self.items) == self.header_buffer_condition:
                    if self.header_buffer_validation_predict(self.items):
                        self.state = SessionState.BodyBuffering
                        self.on_header_buffer_validated(self.items, True)
                    else:
                        self.on_header_buffer_validated(
                            self.items, False)
                        self.reset()
            elif self.header_buffer_type == BufferType.ByPeriodTime:
                def header_buffer_fullfilled():
                    if self.header_buffer_validation_predict(self.items):
                        self.state = SessionState.BodyBuffering
                        self.on_header_buffer_validated(self.items, True)
                    else:
                        self.on_header_buffer_validated(
                            self.items, False)
                        self.reset()
                threading.Timer(self.header_buffer_condition,
                                header_buffer_fullfilled).start()

        if self.state == SessionState.BodyBuffering:
            if self.body_buffer_validation_predict(self.items, item):
                self.items.append(item)
                if self.header_buffer_type == BufferType.ByPeriodTime:
                    if self.session_truncate_timer:
                        self.session_truncate_timer.cancel()
                    self.session_truncate_timer = threading.Timer(
                        self.session_end_with_fixed_time_from_last_item, self.__on_session_truncated__)
                    self.session_truncate_timer.start()

        if self.state == SessionState.InPreSilentTime or self.state == SessionState.SessionEnded or self.state == SessionState.InPostSilentTime:
            pass
