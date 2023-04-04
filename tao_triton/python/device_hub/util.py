import datetime
import json
from typing import List


lastReadjsonConfigFileTime = None
lastReadjsonConfigFileContent = None


def read_fast_from_app_config(path: List[str]):
    global lastReadjsonConfigFileTime
    global lastReadjsonConfigFileContent
    if lastReadjsonConfigFileTime is None or (datetime.datetime.now() - lastReadjsonConfigFileTime).seconds > 5:
        f = open('app_config.json')
        data = json.load(f)
        lastReadjsonConfigFileContent = data
        lastReadjsonConfigFileTime = datetime.datetime.now()
        f.close()
    if path is None:
        return lastReadjsonConfigFileContent
    else:
        for item in path:
            if item in lastReadjsonConfigFileContent:
                lastReadjsonConfigFileContent = lastReadjsonConfigFileContent[item]
            else:
                return None
        return lastReadjsonConfigFileContent


def read_fast_from_app_config_to_board_control_level(path_to_board_control_level_array: List[str], board_id: str):
    board_control_level_configs = read_fast_from_app_config(
        path_to_board_control_level_array)
    if board_control_level_configs:
        current_board_configs = [c for c in board_control_level_configs if
                                 c["TargetBoardId"] == board_id]
        if len(current_board_configs) > 0:
            enable_for_current_board_config = current_board_configs[0]["Enable"]
            return enable_for_current_board_config
        else:
            all_boards_switcher_configs = [c for c in board_control_level_configs if
                                           c["TargetBoardId"] == "*"]
            if len(all_boards_switcher_configs) > 0:
                enable_for_all_board_config = all_boards_switcher_configs[0]["Enable"]
                return enable_for_all_board_config
    return None


def read_fast_from_app_config_to_property(path_to_property: List[str], property_name: str):
    property_level_config = read_fast_from_app_config(
        path_to_property)
    if property_level_config:
        if property_name in property_level_config:
            return property_level_config[property_name]
    return None
