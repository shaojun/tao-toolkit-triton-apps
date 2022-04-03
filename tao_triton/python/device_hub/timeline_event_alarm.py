from enum import Enum
from typing import List


class EventAlarmPriority:
    VERBOSE = 1
    DEBUG = 2
    INFO = 3
    WARNING = 4
    ERROR = 5
    FATAL = 6


class EventAlarm:
    def __init__(self, priority: EventAlarmPriority, description: str):
        self.priority = priority
        self.description = description

    def get_board_id(self):
        return self.board_id


class EventAlarmNotifierBase:
    def __init__(self):
        pass

    def notify(self, alarms: List[EventAlarm]):
        pass


class EventAlarmConsolePrintNotifier:
    def __init__(self):
        pass

    def notify(self, alarms: List[EventAlarm]):
        if not alarms:
            return
        for a in alarms:
            print("alarm with priority:{} - {}".format(a.priority, a.description))
