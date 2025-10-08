from core import SystemCondition ,Ess
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
system_condition = SystemCondition(env_config["redundant_name"])

system_scheduler = BackgroundScheduler()
system_scheduler.add_job(system_condition._check_servers_condition,'interval',seconds=2)
system_scheduler.start()

ess = Ess(deviceConfig=args.device,envConfig=args.config,alertsConfig=args.alert,system_status=system_condition)
ess.start()

