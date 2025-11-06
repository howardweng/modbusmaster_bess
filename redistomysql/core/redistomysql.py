from core.distributor import Distributor
from core.reliability import SystemCondition



import json
from datetime import datetime
from time import sleep
from util import utils
from dbutils.pooled_db import PooledDB
import pymysql
import numpy as np
from collections import defaultdict
from multiprocessing import Pipe

class RedisToMysql:
    def __init__(self,device_info_path,env_info_path,system_status,redis_details,current_server):
        self._current_server = current_server
        self.device_info_path = device_info_path
        with open(self.device_info_path,"r") as f:
            self.device_info = json.load(f)
            if self._current_server == "BTR_solar_r1":
                self.device_info["DBhost"] = self.device_info["DBhost"][0]
                self.device_info["DBpw"] = self.device_info["DBpw"][0]
            else:
                self.device_info["DBhost"] = self.device_info["DBhost"][1]
                self.device_info["DBpw"] = self.device_info["DBpw"][1]
            print(self.device_info["DBhost"])
            print(self.device_info["DBpw"])
            print(self.device_info["DBuser"])
            print(self.device_info["DBport"])
        self.env_info_path = env_info_path
        with open(self.env_info_path,"r") as f:
            self.env_info = json.load(f)
        
            
        self.utils = utils(self.device_info_path,self._current_server)
        self.ipc_num = 2
        self.system_status = system_status
        self.redis_details = redis_details
        self.connection_pool = None

    def get_groups(self):
        db_details = {}
        for key in list(self.device_info.keys()):
            if not isinstance(self.device_info[key], dict):
                db_details[key] = self.device_info.pop(key)
        # db_details["DBport"] = self.config.pop("DBport")
        groups = defaultdict(dict)
        process_3 =  {}

        for device in self.device_info.keys():
            # print(device)
            slave_list = list(self.device_info[device].keys())
            split_size =  self.ipc_num if self.ipc_num < len(slave_list) else len(slave_list)
            group_keys = np.array_split(slave_list, split_size)
            if len(group_keys) > 1:
                for i,group_key in enumerate(group_keys):
                    group = defaultdict(dict)
                    group["data"] = { **db_details}
                    for slave in group_key:
                        # print(slave)
                        group['data'][str(slave)] = self.device_info[device][str(slave)]
                    if str(i+1) in "r1":
                        group['need_observation'] = True
                        group['intial_condition'] = True
                    else:
                        group['need_observation'] = True
                        group['intial_condition'] = False    
                    groups[f"process_{i}"].update(group.copy())
            else:
                # print(device,group_keys[0][0])
                process_3[device] = group_keys[0][0]
        else :
                if process_3:    
                    # print(process_3.keys())
                    group = defaultdict(dict)
                    group["data"] = { **db_details}
                    for device,slave in process_3.items():
                        group['data'][str(slave)] = self.device_info[str(device)][str(slave)]
                    if str(i+1) in "r1":
                        group['need_observation'] = True
                        group['intial_condition'] = True
                    else:
                        group['need_observation'] = True
                        group['intial_condition'] = False    
                        # print(group.keys())
                    groups[f"process_{i+1}"].update(group.copy())
        # print(groups["process_0"]["data"].keys())
        # print(groups["process_1"]["data"].keys())
        # print(groups["process_2"]["data"].keys())
        return groups
    def get_groups_old(self):
        db_details = {}
        db_details["DBhost"] = self.device_info.pop("DBhost")
        db_details["DBuser"] = self.device_info.pop("DBuser")
        db_details["DBpw"] = self.device_info.pop("DBpw")
        db_details["DBport"] = self.device_info.pop("DBport")
        groups = defaultdict(dict)
        process_3 =  {}
        for device in self.device_info.keys():
            slave_list = list(self.device_info[device].keys())
            split_size =  self.ipc_num if self.ipc_num < len(slave_list) else len(slave_list)
            group_keys = np.array_split(slave_list, split_size)
            # print(group_keys)
            if len(group_keys) > 1:
                for i,group_key in enumerate(list(group_keys)):
                    group = defaultdict(dict)
                    for slave in list(group_key):
                        # print(slave)
                        group['data'][slave] = self.device_info[device][slave]
                    if str(i+1) in self._current_server:
                        group['need_observation'] = True
                        group['intial_condition'] = True
                    else:
                        group['need_observation'] = True
                        group['intial_condition'] = False    
                    groups[f"process_{i}"].update(group.copy())
            else:
                group = defaultdict(dict)
                for slave in group_key:
                    group['data'][slave] = self.device_info[slave]
                if str(i+1) in self._current_server:
                    group['need_observation'] = True
                    group['intial_condition'] = True
                else:
                    group['need_observation'] = True
                    group['intial_condition'] = False    
                groups.append(group)
                
        return groups
    def assign_job(self):
        processes = {}
        parents = {}
        current_redis_details = self.redis_details[self._current_server]
        # print(current_redis_details)
        device_groups = self.get_groups()
        # print(device_groups)
        
        for i,group in enumerate(device_groups.values()):
       
            parent ,child = Pipe()
            process = Distributor(group["data"],group["need_observation"],group["intial_condition"],child,current_redis_details,self.connection_pool,self.env_info["log_path"])
            process.daemon = True
            process.start()
            parents[f'process_{i+1}'] = parent
            processes[f'process_{i+1}'] = process
            print("process",i+1,"started")
        return parents,processes
     
            
    def run(self):
        parents,processes = self.assign_job()

        print(parents,processes)
        other_process_controller,current_process_controller = (parents["process_2"],parents["process_1"]) if "r1" in self._current_server else (parents["process_1"],parents["process_2"])
        while True:
            is_ess_alive,working_server = self.system_status.get_system_condition()
            # print(is_server_alive,is_ess_alive,working_server)
            current_process_controller.send("start")
            if len(parents) == 3:
                parents["process_3"].send("start")
            if  not is_ess_alive:
                other_process_controller.send("start")
            else:
                other_process_controller.send("stop")
           
            sleep(1)

    def start(self):
        if True:
            
            try:
                self.utils.create_db()

                print("created db")
                self.utils.create_table()
                print("created table")
                self.connection_pool = PooledDB(pymysql,40,host=self.device_info["DBhost"],user=self.device_info["DBuser"],passwd=self.device_info["DBpw"],port=self.device_info["DBport"])
                print("db connectdd",self.connection_pool)
                print(self.device_info["DBhost"],self.device_info["DBuser"],self.device_info["DBpw"],self.device_info["DBport"])
                # for key in self.config.keys()[:]:
                #     if not isinstance(self.config[key], dict):
                #         self.config.pop(key)
                # print(self.connection_pool)
                # print('going to run')
                self.run()
            except Exception as e:
                # console_logger.error(f"{datetime.now()} - start() - {e}")
                print(e)
                return
        else:
            console_logger.error(f"{datetime.now()} - start() - config error")
            return
                        
                
            