from core import SystemCondition ,Ess
import os
import subprocess
import json 
import argparse
from apscheduler.schedulers.background import BackgroundScheduler

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", type=str, default="config/envConfig.json")
parser.add_argument("-d", "--device", type=str, default="config/deviceConfig.json")
parser.add_argument("-a", "--alert", type=str, default="config/EMStrongAlert.csv")
args = parser.parse_args()

with open(args.config) as f:
    env_config = json.load(f)

if not os.path.exists(env_config["log_path"]):
    os.path.mkdir(env_config["log_path"])
command = "hostname"
Nodes = env_config["Nodes"]
out = subprocess.run(command,shell=True,capture_output = True,text= True).stdout.strip("\n")
if out== env_config["node"]+"devr2":
    Nodes.reverse()
current_server,reduanant_server = Nodes

system_condition = SystemCondition(reduanant_server,node_name=env_config["node"],loc=env_config["loc"])

system_scheduler = BackgroundScheduler()
system_scheduler.add_job(system_condition._check_servers_condition,'interval',seconds=2)
system_scheduler.start()

ess = Ess(deviceConfig=args.device,envConfig=args.config,alertsConfig=args.alert,system_status=system_condition,current_server = current_server)
ess.start()

