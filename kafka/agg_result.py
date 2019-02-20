import os
import datetime
import time
import json
import common

result_directory = "./result"
output_directory = "./output"
offset = 0
result_days = 3
file_arr = []
topic_info = {}

# delete result directory if exist
if os.path.exists(result_directory):
    os.removedirs(result_directory)

# now create result directory
os.makedirs(result_directory)  

# init offset value
offset=time.time()-result_days*24*60*60

# list all the file in the output directory
def list_output_files(start_path):
    for _, _, filenames in os.walk(start_path):
        for f in filenames:
            file_timestamp=int(str(f).split(".")[0])
            if file_timestamp>=offset:
                file_arr.append(str(f))
            
    file_arr.sort(reverse = True)

list_output_files(output_directory)

# store topic detail
def store_topic_detail(name,pu):
    if name not in topic_info:
        uts = []
        topic_info[name] = uts
    uts=topic_info[name]
    uts.append(pu)
    topic_info[name] = uts
     
# parse content of file
def parse_content(content):
    rm = json.loads(content)
    topics=rm["topics"]
    for topic in topics:
        name = topic["name"]
        pu = topic["percent_utilization"]
        store_topic_detail(name, pu)


# get average of a list 
def average(lst): 
    return sum(lst) / len(lst) 

# take average utilization of topic
def topic_average():
    topic_output = []
    for topic in topic_info:
        ds = topic_info[topic]
        avg_pu = average(ds)
        topic_obj = common.Topic(topic, avg_pu)
        topic_output.append(topic_obj)
    return topic_output

# format output and store result
def format_output(topics,info):
    rm = json.loads(info)
    host_name = rm["host_name"]
    host_ip = rm["host_ip"]
    total_size = rm["total_disk_utilized"]
    inf = common.Output(host_name,host_ip,common.get_current_time(), total_size, topics)
    return json.dumps(inf, default=lambda o: o.__dict__)

# iterate each file and parse data
def process_output_file():
    index = 0
    for file_name in file_arr:
        path = output_directory+"/"+file_name
        f = open(path, "r")
        content = str(f.read())
        if index == 0:
            latest_file_info = content
        index +=1
        f.close()
        parse_content(content)
    topic_output = topic_average()
    final_output = format_output(topic_output,latest_file_info)
    file_name = result_directory+"/"+common.get_file_name()+".json"
    common.create_output_file(file_name, final_output)
        

process_output_file()

