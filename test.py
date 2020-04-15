#! /usr/bin/env python
from datetime import datetime
import json
import os
import random
import sys
from threading import Thread
import time
import uuid
import sched

import numpy as np
import pandas as pd
import requests

OUTPUT_DIR = None

def patch(path, data):
    path = '/' + '/'.join(path)
    try:
        val = requests.patch("https://fredzqm-staging.firebaseio-staging.com/%s.json" % (path), data=json.dumps(data))
        if val.status_code != 200:
            print(val, val.content)
    except Exception as e:
        print("Error", e)

# nLeaves = branch_factor^depth
def getRandomTree(branch_factor, depth, isRandom):
    if depth == 0:
        return "1"
    a = {}
    end = branch_factor
    if isRandom:
        end = random.randint(branch_factor//2, branch_factor//2*3)
    for i in range(end):
        a[str(i)] = getRandomTree(branch_factor, depth-1, isRandom)
    return a

# nLeaves = root_branch_factor*branch_factor^depth
def putRandomData(path_list, end, branch_factor, depth, isRandom):
    chunk = 10000000 // (branch_factor ** (depth - len(path_list)))
    if isRandom:
        end = random.randint(end//2, end//2*3)
    if chunk == 0:
        for i in range(end):
            path_list.append(str(i))
            putRandomData(path_list, branch_factor, branch_factor, depth, isRandom)
            path_list.pop()
    else:
        for start in range(0, end, chunk):
            data = {}
            for i in range(start, min(start+chunk, end)):
                path_list.append(str(i))
                data[str(i)] = getRandomTree(branch_factor, depth - len(path_list), isRandom)
                path_list.pop()
            print('sent /' + '/'.join(path_list) +'/['+str(start)+']' )
            patch(path_list, data)
            print('done /' + '/'.join(path_list) +'/['+str(start)+']' )

class Task():
    def __init__(self, end, branch_factor, depth, randomSeed, suffix):
        self.end = end
        self.branch_factor = branch_factor
        self.depth = depth
        if randomSeed:
            random.seed(randomSeed)
        self.isRandom = randomSeed != None
        self.suffix = suffix
        self.root_path = self.parseRootPath(randomSeed)

    def insertData(self):
        putRandomData([self.root_path], self.end, self.branch_factor, self.depth + 2, self.isRandom)

    def parseRootPath(self, randomSeed):
        s = 'p'
        s += '-'+str(self.end)
        for i in range(self.depth):
            s += '-'+str(self.branch_factor)
        if randomSeed:
            s += '_'+str(randomSeed)
        return s + self.suffix

    def deleteBench(self):
        monitor =  LatencyMonitor(self.root_path)
        monitor.start()
        cmd = "{{ time node lib/bin/firebase.js  database:remove /{path} --debug -y 2>&1 ;}} 2>&1 | tee {dir}/{path}.out".format(dir=OUTPUT_DIR ,path=self.root_path.replace("(old)", "\(old\)"))
        print(cmd)
        os.system(cmd)
        monitor.stop()
        monitor.join()

class LatencyMonitor(Thread):
    def __init__(self, root_path):
        Thread.__init__(self)
        self.root_path = root_path
        self.stopped = False
        self.non_200 = 0
        self.failed = 0
        self.buckets = [0] * 1800000 # at most 30in

    def run(self):
        while not self.stopped:
            try:
                start = datetime.now()
                val = requests.get("https://fredzqm-staging.firebaseio-staging.com/unknown/path.json")
                if val.status_code == 200:
                    end = datetime.now()
                    ms = (end-start).microseconds // 1000
                    self.buckets[ms] += 1
                    print("Probe {start} takes {ms}ms".format(start=str(start), ms=ms))
                else:
                    self.non_200 += 1
                    print("Probe {start} takes {ms}ms, but failed".format(start=str(start), ms=ms))
            except Exception as e:
                self.failed += 1
                print("Probe {start} crashed".format(start=str(start)))
            time.sleep(0.131)
        self.summary()

    def stop(self):
        self.stopped = True

    def summary(self):
        df = pd.DataFrame()
        df['count'] = np.array([self.failed, self.non_200] + self.buckets)
        df.index = np.array([-2, -1] + list(range(0, 1800000, 1)))
        df = df[df['count'] != 0]
        df.to_csv("{dir}/{path}.latency".format(dir=OUTPUT_DIR, path=self.root_path))

def runTest(end, branch_factor, depth, randomSeed=None, suffix=""):
    task = Task(end, branch_factor, depth, randomSeed, "(old)")
    print(str(task.parseRootPath(randomSeed)))
    task.insertData()
    task.deleteBench()
    # task = Task(end, branch_factor, depth, randomSeed, "")
    # print(str(task.parseRootPath(randomSeed)))
    # task.insertData()

if __name__ == "__main__":
    OUTPUT_DIR = sys.argv[1]
    os.system("mkdir -p " + OUTPUT_DIR)

    def suit(randomSeed):
        runTest(100, 1000, 2, randomSeed)
        runTest(100, 100, 3, randomSeed)
        runTest(2, 2, 25, randomSeed)
        runTest(3, 3, 15, randomSeed)
        runTest(4, 4, 12, randomSeed)
        runTest(5, 5, 11, randomSeed)
        runTest(25, 25, 4, randomSeed)
        runTest(50, 50, 4, randomSeed)
        runTest(10, 10, 7, randomSeed)
        runTest(10, 10, 6, randomSeed)
        runTest(100, 10, 6, randomSeed)
        runTest(100, 10, 5, randomSeed)
        runTest(1000, 10, 5, randomSeed)
        runTest(1000, 10, 4, randomSeed)
        runTest(10000, 10, 4, randomSeed)
        runTest(10000, 10, 3, randomSeed)
        runTest(100000, 10, 3, randomSeed)
        runTest(100000, 10, 2, randomSeed)
        runTest(1000000, 10, 2, randomSeed)
        runTest(1000000, 10, 1, randomSeed)
        runTest(10000000, 10, 1, randomSeed)
        runTest(10000000, 10, 0, randomSeed)
        runTest(10000, 10000, 1, randomSeed)

    suit(None)
    suit(20)
    suit(2)
    suit(1)

    print('Done')
