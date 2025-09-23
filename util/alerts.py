import json
import csv
import datetime
import logging
from util import loggers
from util import logger_check_alert
from util.clients import ModbusClient,NewPushClient
from datetime import datetime

class EventHandler:
    def __init__(self):
        self.callbacks = {}

    def _add_callback(self, event_name, callback_function):
        if event_name not in self.callbacks:
            self.callbacks[event_name] = []
        self.callbacks[event_name].append(callback_function)

    def _remove_callback(self, event_name, callback_function):
        if event_name in self.callbacks:
            if callback_function in self.callbacks[event_name]:
                self.callbacks[event_name].remove(callback_function)

    #call back function can have positional args and keyword args
    def trigger_event(self, event_name, *args, **kwargs):
        if event_name in self.callbacks:
            for callback_function in self.callbacks[event_name]:
                #(NOTICE)同一event可以依序callback多個function，若這些function都會return 則可能需要一list來接這些return value
                result = callback_function(*args, **kwargs)
                return result

    def register_event_table(self, eventTable):
        for key in eventTable:
            self._add_callback(key, eventTable[key])


class EventTable:
    def __init__(self,log_path:str,file_name:str):
        self._eventTable = {
            "readModbus": self._callback_readModbus,
            "writeModbus": self._callback_writeModbus
            }
        self.file_name = file_name
        self.log_path = log_path
    def _callback_readModbus(self,filename, DBname, hostname, id, register):
        with open(filename) as f:
            data = json.load(f)
        table_info = {}
        for slave in data.keys():
            if isinstance(data[slave], dict):
                register_keys = list(data[slave].keys())
                if data[slave]["DBname"] == DBname:
                    modbus_host = data[slave]["Modbus_ip"]
                    modbus_port = data[slave]["Modbus_port"]

                for item in register_keys:
                    if isinstance(data[slave][item], dict):
                        id = int(id)
                        with open(f'{self.log_path}/callback_error.txt','a') as f:
                            f.write(f"{item} , {hostname} , {data[slave][item]['slaveID']} , {id}")
                            f.write(f"\n")

                        if item == hostname and data[slave][item]["slaveID"] == id:
                            table_info["slaveID"] = data[slave][item]["slaveID"]
                            table_info["freq"] = data[slave][item]["freq"]
                            table_info["DBtable"] = data[slave][item]['DBtable']
                            table_info["needAlert"] = data[slave][item]['needAlert']

                            register_address = data[slave][item]
                            del register_address["freq"]
                            del register_address["slaveID"]
                            del register_address["DBtable"]
                            del register_address["needAlert"]
                            table_info["register_address"] = register_address

                       
        try:
            register_address_list = ModbusClient(modbus_host,modbus_port).read_registers(table_info)
        except Exception as e:
            with open(f'{self.log_path}/callback_error.txt','a') as f:
                f.write(f"callback_readModbus ERROR : {e}\n")
                f.write(f"callback_readModbus ERROR : {table_info}\n")
        return register_address_list.get(int(register))
    @staticmethod
    def _callback_writeModbus(hostname, port, id, func_code_number, start_address, value):
        master = modbus_tcp.TcpMaster(host= hostname, port= port)
        master.set_timeout(5.0)
        try:
            value = int(value)
            func_code_number = int(func_code_number)
            master.execute(id, func_code(func_code_number), start_address, output_value= value)
            return True
        except Exception as e:
            with open(f'{self.log_path}/callback_error.txt','a') as f:
                f.write(f"callback_writeModbus ERROR : {e}\n")
            return False
 
class AlertsCondition:
    def __init__(self,alertsConfig: str,envConfig: str,deviceConfig:str) -> None:
        super().__init__()
        self.alertsConfig = alertsConfig
        self.envConfig = envConfig
        self.deviceConfig = deviceConfig
    
        self._alerts :list[dict[str,str]]
        self._env_variables:dict[str,str]
        self._device_loctions:dict[str,dict[str,str]]

        self._load_config()
        self._Event_table = EventTable(self._env_variables["log_path"],self.deviceConfig)
        self._event_handler = EventHandler()
        self._event_handler.register_event_table(self._Event_table._eventTable)
        

    def _load_config(self)-> None:
        self._get_env_variables()
        self._get_alerts()
        self._get_device_loctions()

    def _get_env_variables(self)-> None:
       
        with open(self.envConfig,encoding='utf-8') as file:
            self._env_variables = json.load(file)
    def _get_alerts(self)-> None:
        with open(self.alertsConfig,newline='',encoding='utf-8') as csvfile:
            self._alerts = list(csv.DictReader(csvfile))
    def _get_device_loctions(self)-> None:
        self._device_loctions = {device_alert['hostname']:{"location":device_alert['location'],"device":device_alert['device']} for device_alert in self._alerts}
    @staticmethod
    def _should_trigger_alert(alert_setting:dict, value):
  
        try:
            threshold = alert_setting["threshold"]
            if not threshold.isnumeric():
                return False

            if isinstance(value, (int, float)):
                threshold = type(value)(threshold)

            if alert_setting["type"] == "Boolean":
                return value == threshold
            elif alert_setting["type"] == "<":
                return value < threshold
            elif alert_setting["type"] == "=":
                return value == threshold
            elif alert_setting["type"] == ">":
                return value > threshold
            return False
        except Exception as e:
            logger_check_alert.error(f"error at should_trigger_alert() : {e}")
    @staticmethod
    def _create_alert_info(hostname:str="", slaveID:str="", register:str="", location:str="", device:str="", condition:str="", level:str="", type:str="", alert_setting_value:str="", need_decode:str="", value:str="", set_pcs:bool=False, pcs_id:list=[]):
        alert_info ={
                    'hostname': hostname,
                    'slaveID': slaveID,
                    'register': register,
                    'location':location, # 地點
                    'device': device, # 設備 UPS IPC 消防等等
                    'condition':condition, #異常、故障、消防一級故障等等
                    'level': level, # 告警等級 預警、告警、保護、故障
                    'value': "" if type == "Boolean" and alert_setting_value == "" else value,
                    'alert_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'set_pcs':set_pcs,
                    'pcs_id':pcs_id,
                    'need_decode':need_decode
                }
        return alert_info
    def _pre_send_alert(self,alert_type:str,alert_info:dict):
        try:
            hostname = alert_info["hostname"]
            condition = alert_info["condition"]
            register = alert_info["register"]
            value = alert_info["value"]
            logger_check_alert.info(f"{hostname} - {condition} - {register} - {value}")
            print(self._env_variables)
            push_client = NewPushClient(url=self._env_variables['push_server_IP'])
            push_client.send_alert(alert_type= alert_type, info_dict=alert_info)
            print("-------------------------------------send alert--------------------------------------------")
        except Exception as e:
            logger_check_alert.error(f"Sending alert to PushServer failed!! {e}") 
    def check_alert(self,alert_type:str,name:str,slaveID:str,register_address_list:dict,filename:str)-> None:
        try:
            for alert_setting in self._alerts:
                if alert_type == "conn":
                    if name == alert_setting["hostname"] and slaveID == alert_setting["slaveid"] and not slaveID.isnumeric():
                        if "BESS" not in name:
                            name = name.split("_",1)[1]
                        else:
                            name = name.split("_",1)[0]

                        condition = deepcopy(alert_setting["condition"])
                        if "alert_msg" in kwargs.keys():
                            condition = (alert_setting["condition"] + f"({kwargs['alert_msg']})") if alert_setting["condition"]!='' else f"{kwargs['alert_msg']}"

                        if alert_setting['set_pcs_list']:
                            set_pcs_list = [pcs for pcs in alert_setting['set_pcs_list'].split(",")]
                            alert_info = self._create_alert_info(name, alert_setting["slaveid"], alert_setting["register"], alert_setting["location"],
                                                        alert_setting["device"], condition, alert_setting["level"], alert_setting["type"],
                                                        alert_setting["value"], "", "", True, set_pcs_list)
                        else:
                            alert_info = self._create_alert_info(name, alert_setting["slaveid"], alert_setting["register"], alert_setting["location"],
                                                        alert_setting["device"], condition, alert_setting["level"], alert_setting["type"],
                                                        alert_setting["value"])
                        self._pre_send_alert("conn", alert_info)
                elif alert_type == "send_alert":
                    # print(alert_type)
                    # print(name , alert_setting["hostname"] , slaveID , alert_setting["slaveid"])
                    if name == alert_setting["hostname"] and slaveID == alert_setting["slaveid"]:
                        # print(name , alert_setting["hostname"] , slaveID , alert_setting["slaveid"])
                      
                        value = register_address_list.get(int(alert_setting["register"]))
                        need_decode = alert_setting.get("need_decode","")

                        if self._should_trigger_alert(alert_setting, value):
                            # print("-------------------------------------nedd alert -------------------------------------------")

                            #如果csv中的value欄位有值則需再次讀取slave拿取所需點位的value
                            if alert_setting["value"]:  # alert_setting["value"] = BESS0_rack01_status,2,51 or ENV_ENV0_1_sensor1,1,769
                                hostname = alert_setting["value"].split(",")[0]
                                DBname = hostname.split("_")[0]
                                hostname = hostname.split("_",1)[1]
                                id = alert_setting["value"].split(",")[1]
                                reg = alert_setting["value"].split(",")[2]
                                value = self._event_handler.trigger_event("readModbus", filename, DBname, hostname, id, reg)  #將value改成current_value

                            # 如果set_pcs_list 不為空，則需set PCS
                            if alert_setting['set_pcs_list']:
                                set_pcs_list = [pcs for pcs in alert_setting['set_pcs_list'].split(",")]
                                alert_info =self._create_alert_info(alert_setting["hostname"], alert_setting["slaveid"], alert_setting["register"],
                                                            alert_setting["location"], alert_setting["device"],alert_setting["condition"],
                                                            alert_setting["level"], alert_setting["type"], alert_setting["value"],
                                                            need_decode, str(value), True, set_pcs_list)
                            else:
                                alert_info = self._create_alert_info(alert_setting["hostname"], alert_setting["slaveid"], alert_setting["register"],
                                                            alert_setting["location"], alert_setting["device"],alert_setting["condition"],
                                                            alert_setting["level"], alert_setting["type"], alert_setting["value"],
                                                            need_decode, str(value))
                         
                            self._pre_send_alert("send_alert", alert_info)

                            # 消防二級告警關空調
                            if "消防" in alert_setting["device"] and "二級" in alert_setting["condition"]:
                                # 讀config檔案
                                slave_name = name.split("_",1)[1]
                                slave_name = slave_name.split("_")[0] + " _aircon"    #ENV1_1 改為 ENV1_aircon (因空調在ENV1_aircon內)
                                DBname = name.split("_")[0]
                                with open(filename) as f:
                                    data = json.load(f)
                                for slave in data.keys():
                                    if isinstance(data[slave], dict):
                                        if slave == slave_name:
                                            register_keys = list(data[slave].keys())
                                            modbus_host = data[slave]["Modbus_ip"]
                                            modbus_port = data[slave]["Modbus_port"]
                                            for item in register_keys:
                                                if isinstance(data[slave][item], dict):
                                                    if "aircon" in item:
                                                        id = data[slave][item]["slaveID"]
                                                        aircon_off_value = data[slave][item]["aircon_off_value"]
                                                        func_code_number = 6
                                                        start_address = None
                                                        for key,reg_value in data[slave][item]["DBtable"].items():
                                                            if key == "Remote_aircon":
                                                                start_address = int(reg_value[1])
                                                        if start_address == None:
                                                            logger_check_alert.info(f"Cannot get reg to turn off aircon, might be no 'Remote_aircon' in config")
                                                            return
                                                        # 呼叫callback function 執行寫入modbus register
                                                        logger_check_alert.info(f"start callback: turn off {item}")
                                                        self._event_handler.trigger_event("writeModbus", modbus_host, modbus_port, id, func_code_number, start_address, aircon_off_value)
                                                        logger_check_alert.info(f"end   callback: turn off {item}")
        except Exception as e:
            logger_check_alert.error(f"Error from check alert: {e}")