import logging
import logging.config
import yaml
from unittest import TestCase


class test_enhanced_rottating_file_handler(TestCase):
    def __init__(self, *args, **kwargs):
        super(test_enhanced_rottating_file_handler,
              self).__init__(*args, **kwargs)

    def test_rotating(self):
        import inspect
        import os.path
        import time
        filename = inspect.getframeinfo(inspect.currentframe()).filename
        path = os.path.dirname(os.path.abspath(filename))
        with open(os.path.join(path, 'test_log_config.yaml'), 'r') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)
        logger = logging.getLogger(__name__)
        for i in range(1, 100, 1):
            logger.debug("I'm the row: {} I'm the rowI'm the rowI'm the rowI'm the row".format(i))
            time.sleep(0.1)
