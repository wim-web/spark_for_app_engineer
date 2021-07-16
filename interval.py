import time
import subprocess

import argparse

p = argparse.ArgumentParser()
p.add_argument("second", nargs="?", default=1)
args = p.parse_args()

while(True):
    time.sleep(float(args.second))
    subprocess.call(["sh", "./execute_create_sensor_data.sh"])
