from core import SystemCondition ,RedisToMysql
import os
import subprocess
import json 
import argparse
from apscheduler.schedulers.background import BackgroundScheduler

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", type=str, default="config/envConfig.json")
parser.add_argument("-d", "--device", type=str, default="config/bess_config.json")
args = parser.parse_args()
try:
    with open(args.config) as f:
        env_config = json.load(f)

    if not os.path.exists(env_config["log_path"]):
        os.path.mkdir(env_config["log_path"])
    command = "hostname"
    out = subprocess.run(command,shell=True,capture_output = True,text= True).stdout.strip("\n")
    if out== env_config["node"]+"devr2":
        env_config["Nodes"].reverse()
    current_server,reduanant_server = env_config["Nodes"]

    system_condition = SystemCondition(reduanant_server,node_name=env_config["node"],loc=env_config["loc"])
    
    system_scheduler = BackgroundScheduler()
    system_scheduler.add_job(system_condition._check_servers_condition,'interval',seconds=2)

    system_scheduler.start()

    redis_to_mysql = RedisToMysql(device_info_path=args.device,env_info_path=args.config,system_status=system_condition,redis_details=env_config["redis_details"],current_server = current_server)
    redis_to_mysql.start()
except Exception as e:
    system_scheduler.remove_all_jobs()
    system_scheduler.shutdown()
    print("Error message: ", e)
    quit()


