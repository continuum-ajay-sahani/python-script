#!/usr/bin/python

import socket, time, datetime

# topic structure
class Topic(object):
    name = ""
    percent_utilization = 0
    utilization_size = 0

    # The class "constructor" - It's actually an initializer 
    def __init__(self, name, percent_utilization, utilization_size):
        self.name = name
        self.percent_utilization = percent_utilization
        self.utilization_size = utilization_size


# output structure
class Output(object):
    note = ""
    host_name = ""
    host_ip = ""
    create_time = ""
    total_disk_utilized = 0
    topics = []

    # The class "constructor" - It's actually an initializer 
    def __init__(self, host_name, host_ip, create_time, total_disk_utilized, topics, note):
        self.host_name = host_name
        self.host_ip = host_ip
        self.create_time = create_time
        self.total_disk_utilized = total_disk_utilized
        self.topics = topics
        self.note = note

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

# provide current time
def get_current_time():
    # datatime format 2019-02-20 18:31:23.1234
    date_vals = str(datetime.datetime.now()).split(".")
    return date_vals[0]

# provide current time
def get_current_time_v2():
    # datatime format 2019-02-20 18:31:23.1234
    date_vals = str(datetime.datetime.now()).split(".")
    formatted_vals = date_vals[0].split(" ")
    return formatted_vals[0]+"_"+formatted_vals[1]  

# create unique file name
def get_file_name():
    # 1550681060.6105819
    file_name = str(time.time()).split(".")[0]
    return  file_name + "." +get_current_time_v2()   

# create output file
def create_output_file(file_name,output_data):
    f = open(file_name, "w")
    f.write(output_data)
    f.close()