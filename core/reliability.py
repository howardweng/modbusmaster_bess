import subprocess
import re
import paramiko
from apscheduler.schedulers.background import BackgroundScheduler
import time

import json

from util import error_logger,logtime_logger

class SystemCondition:
    def __init__(self,service_name: str,server_name: str,host: str,user: str) -> None:

        self.service_name = service_name
        self.server_name = server_name
        self.host = host
        self.user = user
        self.is_server_alive :bool = None 
        self.working_server : str = None
        self.is_ess_alive : bool = True
 
    def _check_servers_condition(self):
        try:
            command = "crm status | grep GTR"
            out = subprocess.run(command,shell=True,capture_output=True,text=True).stdout.split("\n")

            if out :
                pattern = r"GTR_1_dev_r1?2?"
                nodes = re.findall(pattern,out[2])

                self.working_server = re.findall(pattern,out[3])[0]
                self.is_server_alive = True if len(nodes) ==2 else False
        except Exception as e:
            error_logger.error(f"Error from run: {e}")
    def _check_ess_condition(self):
        ess_servies = []
        command = f"systemctl is-active {self.service_name}"
        with paramiko.SSHClient() as ssh_shell:
            ssh_shell.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_shell.connect(hostname=self.host, username=self.user)
            strdin,stdout,stderr = ssh_shell.exec_command(command)
            state = stdout.read().decode().strip()
        self.is_ess_alive = True if state == 'active' else False
    def get_system_condition(self):
        return self.is_server_alive,self.is_ess_alive,self.working_server

class CheckConfig:
    def __init__(self,config_path: str):
        self._path   = config_path
        with open(self._path) as file:
            self._data = json.load(file)
    @staticmethod
    def _check_register(range,items) -> bool:
        for row in items:
            if int(row) < int(range[0]) or int(row) > int(range[-1]):
                return False
        return True
    @staticmethod
    def _check_DBtable(DBtable,register_list):
        dt_type = ['BCD'] # datetime有多種編碼的方式，只要符合其一就行
        for column, value in DBtable.items():
            for row in value[1].split(','):
                if 'register_address_list' not in row:
                    if row != "" and row not in register_list.keys():
                        return False
                    if value[0] != "TEXT" and value[0] != "DATETIME" and value[0] != "TIMESTAMP" and (row=="" or register_list[row] != value[0]):
                        if (row=="" or register_list[row]=='INT' or register_list[row]=='INT32' or register_list[row]=='UINT') and (value[0] == "FLOAT" or value[0] == "INT"):
                            pass
                        else:
                            return False
                    if value[0] == "DATETIME" and row!="" and register_list[row] not in dt_type:
                        return False
                    if value[0] == "TIMESTAMP" and row!="" and register_list[row] != 'TIMESTAMP32':
                        return False
        return True    
    def check(self):
        for slave in self._data.keys():
            if isinstance(self._data[slave], dict):
                #第二層
                for item in self._data[slave].keys():
                    if isinstance(self._data[slave][item], dict):
                        DBtable = self._data[slave][item]['DBtable']
                        register_list = {}
                        #第三層
                        for registers in self._data[slave][item].keys():
                            if isinstance(self._data[slave][item][registers], dict) and registers != 'DBtable':
                                #第四層
                                for register in self._data[slave][item][registers].keys():
                                    if register != 'function_code':
                                        #列出table所有的register
                                        for row in register.split(','):
                                            register_list[row]=self._data[slave][item][registers][register]['DataType'][0]

                                        if not self._check_register(registers.split('-'),register.split(',')):
                                            error_logger.error(f'{registers} 裡的register不相符\n')
                                            return False

                        if DBtable != None and not self._check_DBtable(DBtable,register_list):
                            error_logger.error('DBtable 與 register或register type不相符\n')
                            return False

        return True



if __name__ == "__main__":
    system_scheduler = BackgroundScheduler()

    system_condition = SystemCondition("master_pcs.service","GTR_1_dev_r2","192.168.10.47","root")

    system_scheduler.add_job(system_condition._check_servers_condition,'interval',seconds=1)
    system_scheduler.add_job(system_condition._check_ess_condition,'interval',seconds=10)
    system_scheduler.start()
    time.sleep(10)
    while 1:
        print(system_condition.get_system_condition())
        time.sleep(3)
