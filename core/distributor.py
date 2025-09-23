import threading
import multiprocessing
import copy
from multiprocessing import Pipe
import redis
from datetime import datetime,timedelta
from util import AlertsCondition
from util import RedisClient,ModbusClient
import time
import socket
import modbus_tk

from util import error_logger,logtime_logger

class Worker(threading.Thread):
   
    def __init__(self, name, host, port, table_info, expireTime, next_start_time:datetime,alertsConfig,envConfig,deviceConfig:str,event:threading.Event,redis_conn ):
        self.host = host
        self.port = port 
        self.alertsConfig = alertsConfig
        self.envConfig = envConfig
        self.deviceConfig = deviceConfig
        threading.Thread.__init__(self)
        self.name = name
        self.table_info = table_info
        self.expireTime = expireTime
        self.error_count = 0
        # self.master = Modbus_Master(host,port,5)
        self._stop = event  # Create an Event for stopping the thread
        self._event = threading.Event()   # 利用Event控制while loop 的 trigger時間
        self.next_run_time = next_start_time
        self.wait_seconds = 0.0
        self._alert_checker = AlertsCondition(self.alertsConfig,self.envConfig,self.deviceConfig)
        self.redis_conn = redis_conn
        self.redis_client = RedisClient(self.redis_conn)

    def run(self):
        while True:
            self._event.wait(self.wait_seconds)
            self._event.clear()
            
            if self._stop.is_set():    # Check the Event to see if the thread should stop
                logtime_logger.info(f"1 {self.name}")
                try:
                    
                    register_address_list = ModbusClient(self.host,self.port).read_registers(self.table_info)
                    
                    
                    # Main Meter real value*100, change float to int
                    if self.name == 'Meter_MMAIN':
                        register_address_list = self.integerize_MainMeter(register_address_list)
                  
                    if self.table_info["needAlert"]:
                        self._alert_checker.check_alert("send_alert", self.name, str(self.table_info["slaveID"]), register_address_list,"/home/solar/modbus_bess/config/devicesConfig.json")

                    # self.result = f"{self.name}{register_address_list}"

                    # logger_logtime.info(f"3 {self.name}")
                    if self.table_info['DBtable'] is not None:
                        try:
                            # print(self.host,self.port,self.name)
                            self.redis_client.write_to_redis(register_address_list, self.table_info['DBtable'], self.name, self.expireTime)
                        except Exception as e:
                            error_logger.error("%s - %s" % (self.name, str(e)))

                    self.error_count = 0

                #except 錯誤log需要完整的記錄
                except socket.timeout:
                    error_logger.info(("%s - is timeout  --------------------" % self.name))
                    self.error_count += 1
                    if self.error_count > 3 :
                        check_alert("conn", self.name, "A", {}, sys.argv[1])
                except modbus_tk.modbus.ModbusError as e:
                    error_logger.error("%s - %s" % (self.name, str(e)))
                except ConnectionRefusedError as e:
                    error_logger.error("%s - ConnectionRefusedError" % self.name+" %s " % str(e))
                    self.error_count += 1
                    if self.error_count > 3 :
                        check_alert("conn", self.name, "B", {}, sys.argv[1])
                except Exception as e:
                    error_logger.error("%s - %s" % (self.name, str(e)))
                    self.error_count += 1
                    if "No route to host" in str(e):
                        check_alert("conn", self.name, "C", {}, sys.argv[1])
                    else:
                        if self.error_count > 3 :
                            check_alert("conn", self.name, "D", {}, sys.argv[1])

                logtime_logger.info(f"4 {self.name}\n")

                now = datetime.now()
                time_diff = self.next_run_time - now
                self.next_run_time = self.next_run_time + timedelta(seconds=float(self.table_info["freq"]))
                self.wait_seconds = max(0, (time_diff).total_seconds())
            else :
                time.sleep(0.1)
            

    def integerize_MainMeter(self,register_address_list):
        for key in register_address_list.keys():
            register_address_list[key] = register_address_list[key]*100
        return register_address_list

    def resume(self):
        self._stop.set()  
    def pause(self):
        self._stop.clear()
class Assigner(multiprocessing.Process):
    def __init__(self,alertsConfig:dict,envConfig:dict,deviceConfig:str,data:dict,need_observation:int,intial_condition:bool,pipe:Pipe,r:redis.Redis,need_cell_info:bool=False):
        multiprocessing.Process.__init__(self)
        self.alertsConfig = alertsConfig
        self.envConfig = envConfig
        self.deviceConfig = deviceConfig
        self.data = copy.deepcopy(data)
        self.need_observation = need_observation 
        self.intial_condition = intial_condition 
        self.need_cell_info = need_cell_info
        self.main_event = threading.Event()
        self.sub_event = threading.Event()
        if self.need_cell_info:
            self.sub_event.set()
        print(intial_condition)
        if intial_condition:
            self.main_event.set()

        self.pipe = pipe
        self.r = r
        self.thread_list = {}
    def run(self):
            logtime_logger.info(f"distributor started at {datetime.now()}\n")
            slave_list = {}
            for slave in self.data.keys():
               
                if isinstance(self.data[slave], dict):
                    slave_list[slave] = {}
                    # to get Registers address
                    register_keys = list(self.data[slave].keys())

                    # to get slave hostIp address
                    slave_list[slave]["hostIP"] = self.data[slave]["Modbus_ip"]
                    slave_list[slave]["hostPort"] = self.data[slave]["Modbus_port"]

                    # to get redis expire time
                    slave_list[slave]["expireTime"] = self.data[slave]['expireTime']

                    for item in register_keys:
                       
                        if isinstance(self.data[slave][item], dict):
                          
                            slave_list[slave][item] = {}
                            slave_list[slave][item]["slaveID"] = self.data[slave][item]["slaveID"]
                            slave_list[slave][item]["freq"] = self.data[slave][item]["freq"]
                            slave_list[slave][item]["DBtable"] = self.data[slave][item]['DBtable']
                            slave_list[slave][item]["needAlert"] = self.data[slave][item]['needAlert']
                            # if "modbusRead_breakPoint" in self.data[slave][item].keys():
                            #     slave_list[slave][item]["modbusRead_breakPoint"] = self.data[slave][item]['modbusRead_breakPoint']
                            register_address = self.data[slave][item]

                            del register_address["freq"]
                            del register_address["slaveID"]
                            del register_address["DBtable"]
                            del register_address["needAlert"]
                            if "onChange" in register_address.keys():
                                del register_address["onChange"]
                            # if "modbusRead_breakPoint" in register_address.keys():
                            #     del register_address["modbusRead_breakPoint"]
                            slave_list[slave][item]["register_address"] = register_address
                            Thread_startTime_offset = 240000
                            now = datetime.now()
                            start_time = now + timedelta(seconds=3)
                            start_time = start_time.replace(microsecond=Thread_startTime_offset)
                            next_start_time = start_time + timedelta(seconds=float(slave_list[slave][item]["freq"]))
                            
                            event = self.sub_event if 'cell' in self.data[slave]["DBname"] else self.main_event
                           
                            thread = Worker(self.data[slave]["DBname"]+"_"+item, slave_list[slave]["hostIP"], slave_list[slave]["hostPort"], slave_list[slave][item], slave_list[slave]["expireTime"], next_start_time,alertsConfig=self.alertsConfig,envConfig=self.envConfig,deviceConfig=self.deviceConfig,redis_conn=self.r,event=event)

                            self.thread_list[self.data[slave]["DBname"]+"_"+item] = thread
                            # 設定保護執行緒 daemon
                            thread.daemon = True
                            
                            # 讓 thread在:00.000(整秒)開始  --> 有多個item時會導致每個item執行時間拉開 如:0.0 - M0開始執行, 3.0 - M1開始執行, 6.0 - MAUX開始執行
                            # now = datetime.now()
                            # time_diff = (start_time - now).total_seconds()
                            # time.sleep(time_diff)

                            # start thread
                            thread.start()
                            logtime_logger.info(f"{datetime.now()} - {item} started\n")

                            # 拉開每個Thread之間開始的時間
                            # if self.data[slave]['modbusThread_delay']!=None:
                            #      time.sleep(data[slave]['modbusThread_delay'])
            last_condition = ["",""]
            while True:
                if self.need_observation:
                    command = self.pipe.recv()
                    if ["",""] == last_condition :
                        self.main_event.set() if command[0] else self.main_event.clear()
                        self.sub_event.set() if command[1] else self.sub_event.clear()
                        last_condition = command
                    elif last_condition != command :
                        # print(last_condition,command)
                        if last_condition[0] != command[0]:
                            print(f"main_event:{command[0]}")
                            self.main_event.set() if command[0] else self.main_event.clear()
                        if last_condition[1] != command[1]:
                            print(f"sub_event:{command[1]}")
                            self.sub_event.set() if command[1] else self.sub_event.clear()
                        last_condition = command
                time.sleep(1)