#!/usr/bin/python
# Tested with pbs_version 14.2.2.20170505010934 and PBSPro_13.1.2.163512
import os
import json
from sys import path, argv
from datetime import datetime, timedelta
import re
from ClusterShell.Task import task_self, NodeSet
        
cols = "{0:<16}{1:<26}{2:<50}"
f = cols.format 
                
def dump_help():
        print ("""
Drain Time Estimate Tool for comma delimited list of nodes:
                
        help: {0}
                Print this help message
                
        run: {0} {{node,node,node,++}}
                Run on nodes

        """.format(argv[0]))
        exit()

def print_header():
        print f("NODE", "DRAIN TIME ESTIMATE", "JOB ID")
        print "-------------------------------------------------------------------"
        
def run_task(cmd):
        task = task_self()
        for node in NodeSet('@pbsadmin'):
                task.run(cmd, nodes=node, timeout=60)
                for output, nodelist in task.iter_buffers():
                        if str(NodeSet.fromlist(nodelist)) == node:
                                return str(output)
                return None
                
def get_json(nodes):
                n = ' '.join(nodes)
                cmd = "pbsnodes -H {0} -F json".format(n)
                global jsondata
                jsondata = run_task(cmd)

def get_jobs(n):
        try:
                data = json.loads(jsondata)
                data['nodes'][n]['jobs']
        except:
                return "---"
        data = json.loads(jsondata)
        jobs = data['nodes'][n]['jobs']
        jobs = " ".join(jobs)
        return jobs
        
def get_times(jobs):
        global rtime
        global etime
        global unk
        if "---" not in jobs:
                jobdata = os.popen("clush -Nw chadmin1 qstat -i {0}".format(jobs)).readlines()[5]       #PBS admin node
                etime = jobdata.split()[10]
                rtime = jobdata.split()[8]
        else:
                etime = "drained"
                rtime = "drained"
        if "--" in rtime or "--" in etime:
                unk = "unknown"
                return

def get_dtime(etime, rtime):
        if "---" not in jobs and "--" in rtime or "--" in etime:
                tdelta = "unknown"
                return tdelta
        if "---" not in jobs and "drained" not in etime and "drained" not in rtime:
                fmt = '%H:%M'
                tdelta = datetime.strptime(rtime, fmt) - datetime.strptime(etime, fmt)
                t = str(tdelta)
                t = t.split(":")
                hours = t[0]
                minutes = t[1]
                now = datetime.now().strftime("%Y-%m-%d %H:%M")
                mytime = datetime.strptime(now, "%Y-%m-%d %H:%M")
                mytime += timedelta(hours=int(hours), minutes=int(minutes))
                mytime = mytime.strftime("%Y.%m.%d @ %H:%M")
                return mytime
        else:
                tdelta = "drained"
                return tdelta

###### MAIN ######
if not os.path.isfile("/usr/bin/clush"):
        print "Error: Clush not found (/usr/bin/clush)"
        dump_help()
else:
        next

if argv[1:] and len(argv[1]) >= 6:
        nodes = argv[1].replace(",", " ").split()
        print_header()
        get_json(nodes)
else:
        dump_help()

for n in nodes:
        jobs = get_jobs(n)
        get_times(jobs)
        dtime = get_dtime(etime, rtime)
        print f("%s" % n, "%s" % dtime, "%s" % jobs)
