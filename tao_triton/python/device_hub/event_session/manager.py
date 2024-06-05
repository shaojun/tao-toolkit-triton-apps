import datetime
from enum import Enum
from typing import List
import abc

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
        self.state = SessionState.OPEN
        self.open_time = datetime.datetime.now(datetime.timezone.utc)
        self.close_time = None
        self.state_change_listeners = []

    def regist_state_change_listener(self, listener: SessionStateChangeListener):
        self.state_change_listeners.append(listener)

    def unregist_state_change_listener(self, listener: SessionStateChangeListener):
        self.state_change_listeners.remove(listener)

    @property
    def state(self) -> SessionState:
        return self.state

    @property
    def open_time(self):
        return self.open_time

    @property
    def close_time(self):
        return self.close_time
    
    @abc.abstractmethod
    def feed(self, timeline_items: List):
        pass


class ElectricBicycleInElevatorSession(SessionBase):
    def __init__(self):
        super().__init__(SessionType.ElectricBicycleInElevatorSession)

    @abc.override
    def feed(self, timeline_items: List):
        # raw_infer_results = tao_client.callable_main(['-m', 'elenet_four_classes_230722_tao',
        #                                                   '--mode', 'Classification',
        #                                                  '-u', self.infer_server_ip_and_port,
        #                                                   '--output_path', './',
        #                                                   temp_cropped_image_file_full_name])
        #     infered_class = raw_infer_results[0][0]['infer_class_name']
        #     infer_server_current_ebic_confid = raw_infer_results[0][0]['infer_confid']
        pass

    def get_last_eb_image(self):
        pass

class PersonInElevatorSession(SessionBase):
    def __init__(self):
        super().__init__(SessionType.PersonInElevatorSession)

    @abc.override
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
