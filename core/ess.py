from multiprocessing import Process,Pipe
import numpy as np
from core import CheckConfig,SystemCondition
from core.distributor import Assigner
import json 
import redis 
from collections import defaultdict
import time
from datetime import datetime,timedelta

from util import error_logger,logtime_logger


class Ess:
    def __init__(self,deviceConfig: str,envConfig: str,alertsConfig: str,system_status,ipc_num:int = 2):
        self._deviceConfig = deviceConfig
        self._envConfig = envConfig
        self._alertsConfig = alertsConfig
        self._check_config = CheckConfig(self._deviceConfig)
        self._devices_info = None
        self._env_info = None
        self._load_config()
        self.system_status = system_status
        self.ipc_num = ipc_num
        self.r = None
        self.need_cell_info = False
    def _load_config(self):
        with open(self._deviceConfig) as f:
            self._devices_info = json.load(f)
        with open(self._envConfig) as f:
            self._env_info = json.load(f)
    def get_groups(self):
        groups = []
        redis_details = {}
        redis_details["Redishost"] = self._devices_info.pop("Redishost")
        redis_details["Redisport"] = self._devices_info.pop("Redisport")
        redis_details["Redispw"] = self._devices_info.pop("Redispw")
        slave_list = list(self._devices_info.keys())
        split_size =  self.ipc_num if self.ipc_num < len(slave_list) else len(slave_list)
        group_keys = np.array_split(slave_list, split_size)
        for i,group_key in enumerate(group_keys):
            group = defaultdict(dict)
            group["data"] = { **redis_details}
            for slave in group_key:
                group['data'][slave] = self._devices_info[slave]
            if "r"+str(i+1) in self._env_info["server_name"]:
                group['need_observation'] = True
                group['intial_condition'] = True
            else:
                group['need_observation'] = True
                group['intial_condition'] = False    
            groups.append(group)
        return groups
    def job(self):
        processes = {}
        pipes = {}
        device_groups = self.get_groups()
        for i,group in enumerate(device_groups):
            parent_conn, child_conn = Pipe() 
            # process = Assigner(self._alertsConfig,self._envConfig,self._deviceConfig,group["data"],group["need_observation"],group["intial_condition"],child_conn,self.r)
            process = Assigner(alertsConfig=self._alertsConfig,envConfig=self._envConfig,deviceConfig=self._deviceConfig,data=group["data"],need_observation=group["need_observation"],intial_condition=group["intial_condition"],pipe=child_conn,r=self.r,need_cell_info=self.need_cell_info)
            process.daemon = True
            process.start()
            logtime_logger.info(f"process_{i+1} started")
            pipes[f"process_{i+1}"] = parent_conn
            processes[f"process_{i+1}"] = process
        
        return pipes,processes

    def run(self):
        parents,processes = self.job()
        time.sleep(4)
        logtime_logger.info("sucessfullly assigned jobs ")
        if len(parents.keys()) == 2:
            other_process_controller,current_process_controller = (parents["process_2"],parents["process_1"]) if "r1" in self._env_info["server_name"] else (parents["process_1"],parents["process_2"])
            while True:
                is_server_alive,is_ess_alive,working_server = self.system_status.get_system_condition()
                logtime_logger.info(f"is_server_alive:{is_server_alive},is_ess_alive:{is_ess_alive},working_server:{working_server}")
                print(is_server_alive,is_ess_alive,working_server)
                current_process_controller.send([True,self.need_cell_info])
                if not is_server_alive or not is_ess_alive:
                    #ToDo implent alert 
                    # print("other device data")
                    other_process_controller.send([True,self.need_cell_info])
                else:
                    other_process_controller.send([False,self.need_cell_info])
                
                time.sleep(1)
        
        else:
            while True:
                is_server_alive,is_ess_alive,working_server = self.system_status.get_system_condition()
                logtime_logger.info(f"is_server_alive:{is_server_alive},is_ess_alive:{is_ess_alive},working_server:{working_server}")
                
                if working_server == self._env_info["server_name"] :
                   
                    parents["process_1"].send([True,True])
                else:
                    parents["process_1"].send([False,False])
                time.sleep(1)
    def start(self):
        if self._check_config.check():
            logtime_logger.info("Config check passed")
            try:
                redisPool = redis.ConnectionPool(host=self._devices_info["Redishost"], port=self._devices_info["Redisport"], password=self._devices_info["Redispw"], db=0, decode_responses=True)
                self.r = redis.Redis(connection_pool=redisPool)
                logtime_logger.info("Redis connection established")
            except Exception as e:
                error_logger.error(f"Error from run: {e}")
                return 
            self.run()
        else:
            error_logger.error("Config check failed,Please check config file")
            return 

if __name__== "__main__":
    systemccondition = SystemCondition(service_name="master_pcs.service",server_name="GTR_1_dev_1",host="192.168.10.47",user = "root")

    ess  = Ess("config/bess_config.json",system_status=systemccondition)
    ess.start()
