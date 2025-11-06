
from util import RedisScan ,console_logger,error_logger
from collections import defaultdict
from datetime import datetime,timedelta 
from multiprocessing import Process, Pipe
from apscheduler.schedulers.background import BackgroundScheduler
from dbutils.pooled_db import PooledDB
from pymysql import connect

class DbWriter:
    def __init__(self,tables_info,connection_pool,current_data:dict = defaultdict(list),last_insert_time:dict = defaultdict(bool),redis_details:dict = defaultdict(bool),log_path:str = "/home/brahmareddy-trone/Desktop/Solar/redistomysql/log"):
        self.tables_info = tables_info
        self.current_data = current_data
        self.last_insert_time = last_insert_time
        self.redis_details = redis_details
        self.log_path = log_path
        self.connection_pool = connection_pool
        self.RedisScan = RedisScan(self.log_path)
    
    def choose_data(self,result:list):
        if not result:
            console_logger.debug(f"result is empty no need choose_data")
            return result

        result.reverse()

        new_result = []
        #Store previous data's "second" and when the next "second" changed, store the data into final result
        #like:  "2023-04-11 10:00:00.000" preDateSec = 0, nowDateSec = 0, changeTimes = 0   , store 1 data at the first
        #       "2023-04-11 10:00:00.500" preDateSec = 0, nowDateSec = 0, changeTimes = 0
        #       "2023-04-11 10:00:01.000" preDateSec = 0, nowDateSec = 1, changeTimes = 0 +1, store data
        #       "2023-04-11 10:00:01.500" preDateSec = 1, nowDateSec = 1, changeTimes = 1
        #       "2023-04-11 10:00:02.500" preDateSec = 1, nowDateSec = 2, changeTimes = 1 +1, store data
        # print(result[0])
        predate = datetime.strptime(result[0]["EventTime"], "%Y-%m-%d %H:%M:%S")
        preDateSec = predate.second
        changeTimes = 0 #Use changeTimes to control how much data do we need (as long as changeTimes == time_sec, stop the function)
        new_result.append(result[0])
        try:
            for i in range(100):
                if i == len(result) or not result[i]:   #Redis中的資料數量可能不足(modbusMaster讀取並寫入可能有誤: 如 device connection error)
                    break
                date = datetime.strptime(result[i]["EventTime"], "%Y-%m-%d %H:%M:%S")
                nowDateSec = date.second
                if preDateSec != nowDateSec and changeTimes < (50 - 1):
                    new_result.append(result[i])
                    preDateSec = nowDateSec
                    changeTimes += 1
                elif changeTimes >= 50:
                    changeTimes = 0
                    break
        except Exception as e:
            error_logger.error(f"choose_data() error - result_len:{len(result)}, i:{i} {e}")

        new_result.reverse()
        return new_result
    def read_redis(self):
        with open(f'{self.log_path}/get_data_from_redis_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a') as log_file:
            log_file.write(f'1 {datetime.now()} Start get_data_from_redis\n')
        register_keys = list(self.tables_info.keys())
        db_name = self.tables_info["DBname"]+"_"
        for key in register_keys:
            if isinstance(self.tables_info[key], dict):
                with open(f'{self.log_path}/get_data_from_redis_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a') as log_file:
                    log_file.write(f'2 {datetime.now()} Start {key} RedisScan\n')

                result = self.RedisScan.RedisScan(host=self.redis_details["host"], port=self.redis_details["port"],
                        password=self.redis_details["password"], db=self.redis_details["db"], data_name=db_name+key+"_", count=100)

                end_read_redis_time = datetime.now() - timedelta(seconds=1)    #此處會有UTC or UTC+8的問題
                # end_read_redis_time -= timedelta(hours=8)

                with open(f'{self.log_path}/get_data_from_redis_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a') as log_file:
                    log_file.write(f'3 {datetime.now()} End {key} RedisScan\n')

                if self.tables_info["DBname"]=="INVERTER":
                    print(result)
                result = self.choose_data(result)
                if self.tables_info["DBname"]=="INVERTER":
                    print(result)

                self.current_data[db_name+key].extend(result)
        with open(f'{self.log_path}/get_data_from_redis_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a') as log_file:
            log_file.write(f'\n')
    def write_to_mysql(self):
        with open(f'{self.log_path}/redis_write_into_db_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a') as log_file:
            log_file.write(f'1 {datetime.now()} Start redis_write_into_db\n')

        register_keys = list(self.tables_info.keys())
        db_name = self.tables_info["DBname"]+"_"

        for key in register_keys:
            if isinstance(self.tables_info[key], dict):
                data_ready_to_mysql = []

                # 去除Redis讀出來的資料列表中重複的資料(依照EventTime)
                event_time_list = set() #用來儲存已出現的EventTime
                unique_data_list = []
                for row in self.current_data[db_name+key]:
                    event_time = row["EventTime"]
                    if event_time not in event_time_list:
                        unique_data_list.append(row)
                        event_time_list.add(event_time)
                self.current_data[db_name+key] = unique_data_list

                # 獲取需要存入MySQL的資料(透過EventTime判斷)，並刪除不需要的資料(使用deepcopy->邊loop邊刪除list element的index問題)
                for row in self.current_data[db_name+key][:]:
                    this_data_time = datetime.strptime(row["EventTime"], "%Y-%m-%d %H:%M:%S")
                    if not self.last_insert_time[db_name+key]:
                        data_ready_to_mysql.append(row)
                    elif self.last_insert_time[db_name+key] < this_data_time:
                        data_ready_to_mysql.append(row)
                    else:
                        self.current_data[db_name+key].remove(row)

                # 如果沒有資料需寫入則跳過
                # print(self.tables_info["DBname"])
                if not data_ready_to_mysql:
                    # print('No data')
                    # console_logger.debug(f"{db_name+key} - result from redis is empty, generate_mysql_insert_queries return")
                    continue

                # 更新timestamp
                self.last_insert_time[db_name+key] = datetime.strptime(data_ready_to_mysql[-1]["EventTime"], "%Y-%m-%d %H:%M:%S")

                # 將要存入MySQL的data 轉為SQL語法的string
                to_sql_data = self.generate_mysql_insert_queries(data_ready_to_mysql,key)
                # print(self.tables_info["DBname"])
                

                data_keys = list(self.tables_info[key]["DBtable"].keys())

                column_names = ','.join(data_keys)
                table_name = self.tables_info["DBname"] + "." + key

                to_sql = f"INSERT INTO {table_name} ({column_names}) VALUES"

                to_sql = to_sql + to_sql_data

                try:
                    # print("about to write")
                    client = self.connection_pool.connection()
                    cur=client.cursor()
                    cur.execute(to_sql)
                    # print("about to commit")
                    client.commit()
                    # print("okay")
                    cur.close()
                    client.close()
                except Exception as e:
                    print(type(e))
                    print("sometindfd faa; fkjlllweruqw;e rklejw",e, "is the error")
                    # error_logger.error(f"{datetime.now()} - redis_write_into_db() - {e}")
                    # self.conn.rollback()
                

        with open(f'{self.log_path}/redis_write_into_db_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a') as log_file:
            log_file.write(f'2 {datetime.now()} End   redis_write_into_db\n\n')
    def generate_mysql_insert_queries(self,data,key):
        
        """
        這個函式用於生成MySQL插入語句，將資料轉換為SQL格式。

        Args:
        data (list): 包含資料的列表，每個元素是一個字典。
        quote_settings (dict): 引用設定，包含表格結構等信息。

        Returns:
        str: 這個函式生成一個mysql語法的string。
        """
        to_sql = ''

        data_date = None
        data_keys = list(self.tables_info[key]["DBtable"].keys())
        for row in data:
            temp = '('
            key_found = False
            for key_name in data_keys:
                if key_name == "SBSPM" or key_name == "roll_SBSPM" or key_name == "hour_SBSPM":
                    continue
                for col_name, value in row.items():
                    if key_name == col_name:
                        key_found = True
                        if key_name == "EventTime":
                            data_date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                        elif key_name == "Freq":
                            freq = float(value)
                        elif key_name == "KW_tot":
                            power = float(value)
                        temp += f"'{value}', " if value!='NULL' else f"{value}, "
                        break
                if not key_found:
                    temp += "NULL, "
                key_found = False

            # 刪除最後兩個字符（多餘的逗號和空格），然後添加右括號
            temp = temp[:-2]
            temp += ")"
            # 將這一筆資料的SQL語句添加到整體SQL語句字符串中
            to_sql += f"{temp},"
        # 刪除最後的逗號
        to_sql = to_sql[:-1]
        return to_sql
    def write_to_db(self):
        self.read_redis()
        self.write_to_mysql()
class Distributor(Process):
    def __init__(self,data:dict,need_observation:bool,initial_condition:bool,pipe:Pipe,redis_details:dict,connection_pool,log_path: str):
        super().__init__()
        self.data = data
    
        self.need_observation = need_observation
        self.initial_condition = initial_condition
        self.pipe = pipe
        self.redis_details = redis_details
        self.connection_pool = connection_pool
        self.scheduler = BackgroundScheduler()
        self.log_path = log_path
    def get_last_insert_time(self,db_name,tables_info):
        last_insert_time = defaultdict(bool)
        for table in tables_info.keys():
            if isinstance(tables_info[table], dict):
                with self.connection_pool.connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT MAX(EventTime) FROM {db_name}.{table}")
                    result = cursor.fetchone()
                    if result:
                        last_insert_time[table] = result[0] if result[0] else None
        # print(db_name ,last_insert_time)
        return last_insert_time
    def run(self):
        try:
            wokers = {}
            db_details = {}
            # print("starting")
            # print(self.data.keys())
            for key in list(self.data.keys()):
                if not isinstance(self.data[key], dict):
                    # print(key)
                    db_details[key] = self.data.pop(key)
            # print(db_details.keys(),self.data.keys())        
            for db in self.data.keys():
                last_insert_time = self.get_last_insert_time(self.data[db]["DBname"],self.data[db])

                db_writer = DbWriter(tables_info=self.data[db],connection_pool=self.connection_pool,last_insert_time=last_insert_time,redis_details=self.redis_details,log_path = self.log_path)

                wokers[db] = self.scheduler.add_job(db_writer.write_to_db,'interval',seconds=30,id=db)
                if self.initial_condition:
                    wokers[db].pause()

            self.scheduler.start()

            while True:
                if self.need_observation:
                    command = self.pipe.recv()
                    if command == "start":
                        for worker in wokers.values():
                            worker.resume() 
                    elif command == "stop":
                        for worker in wokers.values():
                            worker.pause()
        except Exception as e:
            self.scheduler.remove_all_jobs()
            self.scheduler.shutdown()
            # console_logger.error(f"{datetime.now()} - run() - {e}")
            return