import os
import json
import sys
import common

output_directory = "./kafka/output"
kafka_log_path = "/tmp/kafka-logs"
folder_info = {}
folder_arr = []
total_disk_utilized = 0

# fetch input argumnet and init value
def init_input_args():
    global kafka_log_path, output_directory
    if len(sys.argv) > 1:
        kafka_log_path = sys.argv[1]
    if len(sys.argv) > 2:
        output_directory = sys.argv[2]    

init_input_args()

print "Kafka Log Path Directory="+kafka_log_path
print "Script Output Directory="+output_directory

# create output directory
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# get a list of all subdirectories in the current directory
def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

dirs=get_immediate_subdirectories(kafka_log_path)

# get size of a directory
def get_size(start_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

# init total disk size utilized
total_disk_utilized = get_size(kafka_log_path)  

# get topic name from partion folder name kafka
def get_topic_name(partition_folder):
    vals = partition_folder.split("-")
    rc = vals[len(vals)-1]
    topic_name = partition_folder.replace("-"+rc, "")
    return topic_name

# get size of each folder
for folder in dirs:
    path = kafka_log_path+"/"+folder
    folder_size = get_size(path)
    topic_name = get_topic_name(folder)
    if topic_name not in folder_info:
        folder_info[topic_name]=folder_size
        continue

    total_size = folder_info.get(topic_name)
    total_size += folder_size
    folder_info[topic_name] = total_size

# format output and store result
def format_output():
    topics = []
    for topic_name in folder_info:
        value = folder_info[topic_name]
        percent_value = value/total_disk_utilized *100
        percent_value = round(percent_value, 2)
        topic = common.Topic(topic_name, percent_value)
        topics.append(topic)

    host_name, host_ip = common.get_Host_name_IP()
    
    inf = common.Output(host_name,host_ip,common.get_current_time(), total_disk_utilized, topics)
    output_data = json.dumps(inf, default=lambda o: o.__dict__)
    file_name = output_directory+"/"+common.get_file_name()+".json"
    common.create_output_file(file_name,output_data)

format_output()

