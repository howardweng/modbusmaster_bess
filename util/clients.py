import modbus_tk.defines as cst
import socket
import struct
import ctypes
import re
from datetime import datetime
from modbus_tk import modbus_tcp
from util import error_logger,logger_check_alert
import zmq
import json

class RedisClient:
    def __init__(self,redis_conn):
        self._redis_conn = redis_conn
    @staticmethod
    def _create_text(register_address_list,reg_str):
        if reg_str != "":
            reg_list = reg_str.split(',')
            text_list = [f"{reg}:{register_address_list[int(reg)]}" for reg in reg_list]
            return ','.join(text_list)
        else:
            return 'NULL' 
    def write_to_redis(self,register_address_list,db_table,table_name,expire_time = 180):
        key = table_name + '_' + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]#key_name needs to save ms for distinguish data
        pipe = self._redis_conn.pipeline()
        for colName,value in db_table.items():
            if value[0] == "TEXT":
                text = self._create_text(register_address_list,value[1])
            else:
                if value[0] == "DATETIME" and value[1] == "":
                    text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                elif len(value[1].split(',')) != 1:
                    text = self._create_text(register_address_list,value[1])
                elif value[1] == "":
                    text = 'NULL'
                else:
                    # 如果读取value(即register) 不是register，进入 except
                    try:
                        text = str(register_address_list[int(value[1])])
                        pattern = r'^-?\d+(\.\d+)?$'
                        if re.match(pattern, text)==None:# 確定text為數字(利用正则表达式判斷)
                            text = '-999999'
                    except:
                        text = str(eval(value[1]))
            pipe.hset(key, colName, text)
        
        try:
            pipe.expire(key, time= str(expire_time))#259200
            pipe.execute()
        except Exception as e:
            error_logger.error("unable to insert data into redis")
     

class ModbusClient:
    def __init__(self,ip_address: str, port: int,timeout: int = 5):

        self._ip_address = ip_address
        self._port = port
        self._timeout = timeout
       
        #Intilize the mapping table 
        #Later used to map the function code to the corresponding function

        self._mapping_table = {1:cst.READ_COILS,2:cst.READ_DISCRETE_INPUTS,
        3:cst.READ_HOLDING_REGISTERS,4:cst.READ_INPUT_REGISTERS,
        5:cst.WRITE_SINGLE_COIL,6:cst.WRITE_SINGLE_REGISTER,
        15:cst.WRITE_MULTIPLE_COILS,16:cst.WRITE_MULTIPLE_REGISTERS,23:cst.READ_WRITE_MULTIPLE_REGISTERS
        }
        
        """
        Initialize the Modbus client

        """
        # self._connect()
    def _connect(self):
        """
        Connect to the Modbus server

        """
        self._master = modbus_tcp.TcpMaster(host=self._ip_address, port=self._port)
        self._master.set_timeout(self._timeout)
    @classmethod 
    def _get_connection(cls):
        pass
    
    def _code_mapping(self,code: int):
        """
        Map the Modbus function code to the corresponding function

        Args:
            code (int): The Modbus function code

        Returns:
            function: The corresponding function
        """

        code = int(code)
        code = code if code in self._mapping_table else None
        if code is None:
            raise ValueError("Invalid Modbus function code")
        return self._mapping_table.get(code)
    @staticmethod
    def _read_32_bit(register_1: int,register_2: int)-> int:
        """
        Read the value of a register

        Args:
            register_1 (int): The first register
            register_2 (int): The second register

        Returns:
            int: The value of the register
        """
      
        return register_1 << 16 | register_2
    @staticmethod
    def _read_float(register_1: int,register_2: int,number_of_bytes: int = 2,byteorder: str = 'big')-> float:
        """
        Read the value of a register

        Args:
            register_1 (int): The first register
            register_2 (int): The second register
            number_of_bytes (int): The number of bytes to read
            byteorder (str): The byte order

        Returns:
            float: The value of the register
        """
        out = register_1.to_bytes(number_of_bytes, byteorder=byteorder) + register_2.to_bytes(number_of_bytes, byteorder=byteorder)
        out = round(struct.unpack("!f",out)[0],2)
        return out
    @staticmethod
    def _corrected_value(value: int,correction_factor: str)-> int:
        """
        Correct the value of a register

        Args:
            value (int): The value of the register
            correction_factor (float): The correction factor

        Returns:
            int: The corrected value of the register
        """
        if correction_factor != None:
            try:
                for num in correction_factor.split(' '):
                   
                
                    if num[0] == '+' or num[0] == '-':
                        return round(value+float(num),2)
                    else:
                        return round(value*float(num),2)
            except Exception as e:
                error_logger.error(e)
            
    @classmethod
    def get_master(cls):
        """
        Get the Modbus master

        Returns:
            modbus_tcp.TcpMaster: The Modbus master
        """
        return cls._master

    def read_registers(self,table_info: dict) -> dict:
        """
        Read the value of a register

        Args:
            table (dict): The table of registers

        Returns:
            dict: The values of the registers
        """
        register_address_list = {}
        self._master = modbus_tcp.TcpMaster(self._ip_address,self._port)
        
        for key,value in table_info['register_address'].items():
            if value['function_code']==5 or value['function_code']==6 or value['function_code']==15 or value['function_code']==16:
                continue
            start_address = int(key.split('-')[0])
            length = int(key.split('-')[1])-int(key.split('-')[0])+1
            try:

                e1 = self._master.execute(table_info['slaveID'], self._code_mapping(int(value['function_code'])), start_address, length, threadsafe=False)
            
                for regList,regInfo in value.items():

                    if regList != 'function_code':
                        for reg in regList.split(','):
                            startOffset = int(reg)-start_address
                            if regInfo["DataType"][0] == 'INT':
                                real_val = ctypes.c_int16(e1[startOffset]).value
                                # modbus value to real value
                                real_val = self._corrected_value(real_val, regInfo["toRealValue"])
                                register_address_list[int(reg)] = real_val
                            elif regInfo["DataType"][0] == 'UINT':
                                real_val = ctypes.c_uint16(e1[int(reg)-start_address]).value
                                # modbus value to real value
                                real_val = self._corrected_value(real_val, regInfo["toRealValue"])
                                register_address_list[int(reg)] = real_val
                            elif regInfo["DataType"][0] == 'INT32':
                                if regInfo['DataType'][1] == 'L2H':
                                    real_val = ctypes.c_int32(self._read_32_bit(e1[startOffset], e1[startOffset+1])).value
                                elif regInfo['DataType'][1] == 'H2L':
                                    real_val = ctypes.c_int32(self._read_32_bit(e1[startOffset+1], e1[startOffset])).value
                                # modbus value to real value
                                real_val = self._corrected_value(real_val, regInfo["toRealValue"])
                                register_address_list[int(reg)] = real_val
                            elif regInfo["DataType"][0] == 'UINT32':
                                if regInfo['DataType'][1] == 'L2H':
                                    real_val = ctypes.c_uint32(self._read_32_bit(e1[startOffset], e1[startOffset+1])).value
                                elif regInfo['DataType'][1] == 'H2L':
                                    real_val = ctypes.c_uint32(self._read_32_bit(e1[startOffset+1], e1[startOffset])).value
                                # modbus value to real value
                                real_val = self._corrected_value(real_val, regInfo["toRealValue"])
                                register_address_list[int(reg)] = real_val
                            
                            elif regInfo["DataType"][0] == 'FLOAT':
                                if regInfo['DataType'][1] == 'L2H':
                                    real_val = self._read_float(e1[startOffset], e1[startOffset+1])
                                elif regInfo['DataType'][1] == 'H2L':
                                    real_val = self._read_float(e1[startOffset+1], e1[startOffset])
                                # modbus value to real value
                                real_val = self._corrected_value(real_val, regInfo["toRealValue"])
                                register_address_list[int(reg)] = real_val
                            elif regInfo["DataType"][0] == 'BOOLEAN':
                                register_address_list[int(reg)] = e1[startOffset]
                            elif regInfo["DataType"][0] == 'BCD':
                                real_val = BCD_encode(e1[startOffset:startOffset+6])
                                register_address_list[int(reg)] = real_val
                            elif regInfo["DataType"][0] == 'TIMESTAMP32':
                                real_val = TIMESTAMP32_encode([e1[startOffset],e1[startOffset+1]])
                                register_address_list[int(reg)] = datetime.fromtimestamp(real_val)
            except Exception as e :
                error_logger.error("current error ",e)
        return register_address_list

class NewPushClient(object):
    def __init__(
        self,
        url: str = "127.0.0.1",
        port: str = "5553",
        sub_port: str = "5554",
        AccessID: str = "1",
        freq:int = 10, # 幾秒發送一次 request 至 push server
        force_connect:bool = False,
        sub_filter:list = [], #subscribe
        open_subscibe_scoket:bool = False
    ):
        self.port = url + ':' + port
        self.sub_port = url + ':' + sub_port
        self.socket = self.get_device_socket(self.port)
        self.sub_filter = sub_filter
        self.recv_cmd = []
        self.AccessID = AccessID
        self.freq = freq
        self.force_connect = force_connect
        # use poll for timeouts:
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.open_subscibe_scoket = open_subscibe_scoket
        if open_subscibe_scoket:
            self.sub_socket = self.get_sub_socket(self.sub_port, self.sub_filter)
            self.poller.register(self.sub_socket, zmq.POLLIN)

    def get_device_socket(self, port):
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect("tcp://" + port)
        logger_check_alert.info(f"Successfully connected to machine {port}")
        return socket

    def get_sub_socket(self, port, sub_filter:list=[]):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        if not sub_filter:
            socket.setsockopt(zmq.SUBSCRIBE, b"")
            logger_check_alert.info(f"Subscribe all channel.")
        else:
            for filter_name in sub_filter:
                socket.setsockopt(zmq.SUBSCRIBE, str.encode(filter_name))
                logger_check_alert.info(f"Subscribe {str.encode(filter_name)} channel.")
        socket.connect("tcp://" + port)
        logger_check_alert.info(f"Successfully connected to machine {port}")
        return socket

    def send_alert(self,
        alert_type:str = 'send_alert', # send_alert, conn
        info_dict: dict = {
            'hostname': '',
            'slaveID': '',
            'register': '',
            'location':'', # 地點
            'device': '', # 設備 UPS IPC 消防等等
            'condition': '', #異常、故障、消防一級故障等等
            'level': '', # 告警等級 預警、告警、保護、故障
            'value': '', #
            'alert_time': '1999-08-18 00:00:00', #
            'set_pcs': False, # 設定 PCS = 0
            'pcs_id': [], # 設定那些PCS歸零
            'ctrl_device':'', #設定需要控制的設備(如:消防二級告警，須關閉空調系統) example: ENV0_1
            'ctrl_cmd':'', #設定需要控制的指令 example: aircon_off
            'need_decode':'' #設定是否需要decode value(For Fimer PCS)
        }) -> bool:
        self.socket.send_string("", zmq.SNDMORE)  # delimiter
        send_alert = {
            'AccessID': self.AccessID,
            'type': alert_type,
            'hostname':info_dict.get('hostname', ''),
            'slaveID':info_dict.get('slaveID', ''),
            'register':info_dict.get('register', ''),
            'location':info_dict.get('location', ''),
            'device':info_dict.get('device', ''),
            'condition':info_dict.get('condition', ''),
            'level':info_dict.get('level', ''),
            'value':info_dict.get('value', ''),
            'alert_time':info_dict.get('alert_time', ''),
            'set_pcs': info_dict.get('set_pcs', False),
            'set_all': info_dict.get('set_all', False),
            'pcs_id': info_dict.get('pcs_id', []),
            'ctrl_device': info_dict.get('ctrl_device',''),
            'ctrl_cmd': info_dict.get('ctrl_cmd',''),
            'need_decode': info_dict.get('need_decode','')
        }
        logger_check_alert.info(f'Send alert - {send_alert["hostname"]} - {send_alert["condition"]} - {send_alert["value"]}')

        send_alert = json.dumps(send_alert)
        self.socket.send_string(send_alert)  # actual message
        socks = dict(self.poller.poll(5 * 1000))

        if self.socket in socks:
            try:
                self.socket.recv()  # discard delimiter
                msg_json = self.socket.recv()  # actual message
                sens = json.loads(msg_json)
                if sens.get('status') == 'fail':
                    logger_check_alert.info(f'send_alert - Send Alert error')
                    return False
                else: return True
            except IOError:
                logger_check_alert.error(f"send_alert - Could not connect to machine")
                return True
        else:
            logger_check_alert.info(f"send_alert - Machine did not respond")
            return True
