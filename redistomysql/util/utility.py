import json 
import pymysql
import redis
from datetime import datetime,timedelta
import re 



class RedisScan:
    def __init__(self,log_path:str):
        self.log_path = log_path

    @staticmethod
    def atoi(text: str) -> int:
        return int(text) if text.isdigit() else text
    
    
    def natural_keys(self, text: str) -> list:
        return [ self.atoi(c) for c in re.split(r'([0-9]+)', text) ]
    
    def RedisScan(self,host, port, password, db, data_name, count):

        with redis.Redis(host=host, port=port,password=password, db=db, decode_responses=True) as client:
            log_file = open(f'{self.log_path}/redisScanner_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a')
            datas = []
            keylist = []
            log_file.write(f"{datetime.now()} - {data_name} Start Scan_iter\n")

            for key in client.scan_iter(count=5000):
                if data_name in key:
                    keylist.append(key)

            keylist.sort(key=self.natural_keys)

            count = -int(count)
            pipe = client.pipeline()
            for key in keylist[count:]:
                pipe.hgetall(key)
            results = pipe.execute()

            if not results:
                log_file.write(f"{datetime.now()} - {data_name} Scan  nothing!\n\n")
                return results

            i = 0
            for key in keylist[count:]:
                #datas.append(key)
                datas.append(results[i])
                i += 1

            log_file.close()
            for row in datas:
                if not row:
                    datas.remove(row)
            return datas


    def RedisScanByTime(host, port, password, db, data_name, count, delay, time_sec = 10):
        with redis.Redis(host=host, port=port,password=password, db=db, decode_responses=True) as client:
            datas = []
            keylist = []

            for key in client.scan_iter(count=5000):
                if data_name in key:
                    keylist.append(key)

            keylist.sort(key=self.natural_keys)

            count = int(count)
            if (count / delay) % 1 != 0:
                count = int(count / delay) + 1
            else:
                count = int(count / delay)
            pipe = client.pipeline()
            for key in keylist[-count:]:
                pipe.hgetall(key)
            results = pipe.execute()
            results.reverse()

            if not results:
                return results

            predate = datetime.strptime(results[0]["EventTime"], "%Y-%m-%d %H:%M:%S")
            preDateSec = predate.second
            changeTimes = 0 #Use changeTimes to control how much data do we need (as long as changeTimes == time_sec, stop the function)
            datas.append(results[0])

            for i in range(count):
                date = datetime.strptime(results[i]["EventTime"], "%Y-%m-%d %H:%M:%S")
                nowDateSec = date.second
                if preDateSec != nowDateSec and changeTimes < (time_sec - 1):
                    datas.append(results[i])
                    preDateSec = nowDateSec
                    changeTimes += 1
                elif changeTimes >= time_sec:
                    changeTimes = 0
                    break

            datas.reverse()
    
    def RedisScanOneSec(host, port, password, db, data_name, count, time_sec = 10):

        with redis.Redis(host=host, port=port,password=password, db=db, decode_responses=True) as client:
            log_file = open(f'{log_path}/redisScanner_{datetime.now().strftime("%Y-%m-%d")}.txt', 'a')
            datas = []
            keylist = []

            log_file.write(f"{datetime.now()} - {data_name} Start Scan_iter\n")

            for key in client.scan_iter(count=5000):
                if data_name in key:
                    keylist.append(key)

            keylist.sort(key=self.natural_keys)

            count = -int(count)
            pipe = client.pipeline()
            for key in keylist[count:]:
                pipe.hgetall(key)
            results = pipe.execute()
            results.reverse()

            if not results:
                log_file.write(f"{datetime.now()} - {data_name} Scan  nothing!\n\n")
                return results
            #Store previous data's "second" and when the next "second" changed, store the data into final result
            #like:  "2023-04-11 10:00:00.000" preDateSec = 0, nowDateSec = 0, changeTimes = 0   , store 1 data at the first
            #       "2023-04-11 10:00:00.500" preDateSec = 0, nowDateSec = 0, changeTimes = 0
            #       "2023-04-11 10:00:01.000" preDateSec = 0, nowDateSec = 1, changeTimes = 0 +1, store data
            #       "2023-04-11 10:00:01.500" preDateSec = 1, nowDateSec = 1, changeTimes = 1
            #       "2023-04-11 10:00:02.500" preDateSec = 1, nowDateSec = 2, changeTimes = 1 +1, store data
            predate = datetime.strptime(results[0]["EventTime"], "%Y-%m-%d %H:%M:%S")
            preDateSec = predate.second
            changeTimes = 0 #Use changeTimes to control how much data do we need (as long as changeTimes == time_sec, stop the function)
            datas.append(results[0])
            try:
                for i in range(-count):
                    if i == len(results) or not results[i]:   #Redis中的資料數量可能不足(modbusMaster讀取並寫入可能有誤: 如 device connection error)
                        break
                    date = datetime.strptime(results[i]["EventTime"], "%Y-%m-%d %H:%M:%S")
                    nowDateSec = date.second
                    if preDateSec != nowDateSec and changeTimes < (time_sec - 1 ):
                        datas.append(results[i])
                        preDateSec = nowDateSec
                        changeTimes += 1
                    elif changeTimes >= time_sec:
                        changeTimes = 0
                        break
            except Exception as e:
                print(f"{data_name} - len:{len(results)}, i:{i} {e}")

            datas.reverse()
            log_file.close()
            return datas

class utils:
    def __init__(self,file_path:str,current_server:str)->None:
        self.file_path = file_path
        with open(self.file_path,"r") as f:
            self.data = json.load(f)
            if current_server == "BTR_solar_r1":
                self.data["DBhost"] = self.data["DBhost"][0]
                self.data["DBpw"] = self.data["DBpw"][0]
            else:
                self.data["DBhost"] = self.data["DBhost"][1]
                self.data["DBpw"] = self.data["DBpw"][1]
            print(self.data["DBhost"])
            print(self.data["DBpw"])
            print(self.data["DBuser"])
            print(self.data["DBport"])

    
    def create_db(self):
        print("creating dbs")
        for device in self.data.keys():
          
            if isinstance(self.data[device], dict):
                for device_group in self.data[device].keys():
             
                    if isinstance(self.data[device][device_group], dict):
                        
                        db01 = pymysql.connect(host = self.data["DBhost"], user = self.data["DBuser"], passwd = self.data["DBpw"])
                        cursor = db01.cursor()
                        # print(self.data[device][device_group]["DBname"])
                        SQL = "create database if not exists " + self.data[device][device_group]["DBname"] +" ; "
                        cursor.execute(SQL)
                        db01.close()
        print("created dbs")
    @staticmethod
    def get_type(datatype):
        if datatype == "INT" or datatype == "UINT":
            return "INT"
        elif datatype == "INT32" or datatype == "UINT32":
            return "BIGINT"
        elif datatype == "FLOAT":
            return "FLOAT"
        elif datatype == "DOUBLE":
            return "DOUBLE"
        elif datatype == "BOOL":
            return "TINYINT" 
        else:
            return datatype
    def create_table(self):
        # print("creating tables")
        for device in self.data.keys():
            if isinstance(self.data[device], dict):
                for device_group in self.data[device].keys():
                    if isinstance(self.data[device][device_group], dict):
                        for table_name in self.data[device][device_group].keys():
                            if isinstance(self.data[device][device_group][table_name], dict):
                                table_prefix = self.data[device][device_group][table_name]['DBtable']
                                #print(table_prefix)
                                if table_prefix != None:
                                    #print(table_name, "   ", table_prefix)

                                    db01 = pymysql.connect(host = self.data["DBhost"], user = self.data["DBuser"], passwd = self.data["DBpw"])
                                    # db01 = pymysql.connect(host = "192.168.10.44", user = "root", passwd = "Trone-9939")
                                    cursor = db01.cursor()
                                    # print (self.data[device][device_group]["DBname"])
                                    sql = "use " + self.data[device][device_group]["DBname"]
                                    cursor.execute(sql)

                                    sql = "create table if not exists " +str(table_name) + "("
                                    sql += "item int primary key auto_increment, "
                                    sql += "insert_time timestamp default current_timestamp, "
                                    #print(sql)
                                    for columnName, value in table_prefix.items():
                                        sql += columnName   # 先定義欄位名稱
                                        sql += " "
                                        sql += self.get_type(value[0])     # 定義data type
                                        sql += str(", ")

                                    ######## ADD INDEX start ##############
                                    if 'tableINDEX' in self.data[device][device_group][table_name].keys():
                                        index_prefix = self.data[device][device_group][table_name]['tableINDEX']
                                        for indexName, column in index_prefix.items():
                                            sql += "INDEX "
                                            sql += indexName   # 先定義index名稱
                                            sql += " "
                                            sql += column     # 定義欄位
                                            sql += str(", ")
                                    ######## ADD INDEX end ################
                                    
                                    
                                    sql = sql[:-2]        
                                    sql += ")"
                                    # print(sql)
                                    # print(sql)
                                    cursor.execute(sql)
                                    print("created table",table_name)
                                    query =   f"select count(*) from information_schema.statistics where table_schema = '{self.data[device][device_group]['DBname']}' and table_name = '{table_name}' and index_name = 'eventTimeIndex' "
                                    print("query",query)
                                    cursor.execute(query)
                                    result = cursor.fetchone()
                                    print("result",result)
                                    if result[0] == 0:
                                        cursor.execute(f"create index eventTimeIndex on {table_name} (EventTime)")
                                        print("created index",table_name)
                                    print("created index",table_name)
                                    cursor.close()
                                    db01.close()
        print("資料表建立完成...")
