import logging
class enhanced_rotating_file_handler(logging.handlers.TimedRotatingFileHandler, logging.handlers.RotatingFileHandler):
    '''
        cf http://stackoverflow.com/questions/29602352/how-to-mix-logging-handlers-file-timed-and-compress-log-in-the-same-config-f

         Spec:
         Log files limited in size & date. I.E. when the size or date is overtaken, there is a file rollover
     '''

    ########################################


    def __init__(self, filename, mode = 'a', maxBytes = 0, backupCount = 0, encoding = None,
             delay = 0, when = 'h', interval = 1, utc = False):

        logging.handlers.TimedRotatingFileHandler.__init__(
        self, filename, when, interval, backupCount, encoding, delay, utc)

        logging.handlers.RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount, encoding, delay)

     ########################################

    def computeRollover(self, currentTime):
        return logging.handlers.TimedRotatingFileHandler.computeRollover(self, currentTime)

    ########################################

    def getFilesToDelete(self):
        return logging.handlers.TimedRotatingFileHandler.getFilesToDelete(self)

    ########################################

    def doRollover(self):
        return logging.handlers.TimedRotatingFileHandler.doRollover(self)

    ########################################

    def shouldRollover(self, record):
         """ Determine if rollover should occur. """
         return (logging.handlers.TimedRotatingFileHandler.shouldRollover(self, record) or logging.handlers.RotatingFileHandler.shouldRollover(self, record))
