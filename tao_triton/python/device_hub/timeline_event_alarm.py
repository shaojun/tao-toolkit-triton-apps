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
        self.board_id = None

    def get_board_id(self):
        return self.board_id


class EventAlarmNotifierBase:
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        pass


class EventAlarmDummyNotifier:
    def __init__(self, logging):
        self.logger = logging.getLogger(__name__)

    def notify(self, alarms: List[EventAlarm]):
        if not alarms:
            return
        for a in alarms:
            self.logger.info(
                "Notifying alarm from board: {} with priority:{} - {}"
                    .format(a.get_board_id(), a.priority, a.description))
