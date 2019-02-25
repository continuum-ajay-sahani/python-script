#!/usr/bin/python

import socket, time, datetime

# topic structure
class Topic(object):
    name = ""
    percent_utilization = 0

    # The class "constructor" - It's actually an initializer 
    def __init__(self, name, percent_utilization):
        self.name = name
        self.percent_utilization = percent_utilization


# output structure
class Output(object):
    host_name = ""
    host_ip = ""
    create_time = ""
    total_disk_utilized = 0
    topics = []

    # The class "constructor" - It's actually an initializer 
    def __init__(self, host_name, host_ip, create_time, total_disk_utilized, topics):
        self.host_name = host_name
        self.host_ip = host_ip
        self.create_time = create_time
        self.total_disk_utilized = total_disk_utilized
        self.topics = topics

# get hostname and IP address of machine
def get_Host_name_IP(): 
    host_name = ""
    host_ip = ""
    try: 
        host_name = socket.gethostname() 
        host_ip = socket.gethostbyname(host_name) 
        
    except: 
        print("Unable to get Hostname and IP") 

    return host_name, host_ip  

# create unique file name
def get_file_name():
    # 1550681060.6105819
    return str(time.time()).split(".")[0]

# provide current time
def get_current_time():
    # datatime format 2019-02-20 18:31:23.1234
    date_vals = str(datetime.datetime.now()).split(".")
    return date_vals[0]

# create output file
def create_output_file(file_name,output_data):
    f = open(file_name, "w")
    f.write(output_data)
    f.close()