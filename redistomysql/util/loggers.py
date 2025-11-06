import json 
import os
import logging
from logging.handlers import TimedRotatingFileHandler





class Logger:
    def __init__(self, envConfig: str):
        self.envConfig = envConfig
        self._log_path = None
        self._get_env_variables()
        
    def _get_env_variables(self)-> None:
        with open(self.envConfig,encoding='utf-8') as file:
            self._env_variables = json.load(file)

    def _get_log_path(self,folder: str=None)-> None:
        
        self._log_path = self._env_variables['log_path'] + '/' + folder
        if not os.path.exists(self._log_path):
            os.makedirs(self._log_path)
    def get_logger(self,name: str,folder: str=None,level: int = logging.DEBUG,handler:TimedRotatingFileHandler = TimedRotatingFileHandler,fmt: str = "[{asctime}] [{levelname}] {filename}:{lineno} - {message}",style: str = '{')-> None:
        self._get_log_path(folder)
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._handler = handler(f"{self._log_path}/{name}.log", when="midnight")
        self._handler.setFormatter(logging.Formatter(fmt,style=style))
        self._logger.addHandler(self._handler)
        return self._logger


logger = Logger("config/envConfig.json")
error_logger = logger.get_logger("error",folder="error")
logtime_logger = logger.get_logger("logtime",folder="logtime")
logger_check_alert = logger.get_logger("check_alert",folder="check_alert")
console_logger = logger.get_logger("console",folder = "console")


