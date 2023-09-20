# This file was shared by Toni from Telegra
# This file does not include the code to fetch the history data
import time
import datetime
import random
import string
import csv
from csv import DictReader
import json
import requests

g_EntriesPerChid = 36000
g_devices = [19010, 19011, 19012, 19013, 19014, 19015]
g_WriteBatchSize = 10000
g_DataTypeName = "DataType"
g_DataTypeValue = "AVSafetyMetrics"
g_DataListName = "DataList"
g_api_w_url = "http://70.229.15.100:9080/txvapi/history/insertExternalData"
g_api_r_url = "http://70.229.15.100:9080/txvapi/history/getHistoryData"

startTS = time.time()
startTS = int(startTS) + 0.0013333
g_Timestamps = []

g_row_names_types = {'Chid': int, 'TimeStamp': str, 'TTC_type': str, 'TTC': float, 'TET': float, 'TIT': float,
                   'PET_type': str, 'PET_i_minus_1': int, 'PET_i': int, 'PET': float, 'Segment_ID': str, 'LP': float,
                   'Lo_AR': float, 'Jerk': float, 'La_AR': float, 'VD': float, 'DRS': float, 'RSS_type': str,
                   'min_lo_distance': float, 'gap_lo_distance': float, 'min_la_distance': float,
                   'gap_la_distance': float}
    
def make_Dictionary(csvFilePath):
    list_of_dict= list()
    with open(csvFilePath, encoding='utf-8') as csvf:
        dict_reader = DictReader(csvf, delimiter=';')
        for row in dict_reader:
            row_converted = {k: g_row_names_types[k](v) for k, v in row.items()}
            split_dictionary = dict(list(row_converted.items())[0:2])
            d2 = dict(list(row_converted.items())[2:len(row_converted)])
            #tmpstring = json.dumps(d2)
            split_dictionary["JsonData"] = d2
            #split_dictionary["JsonData"] = tmpstring
            list_of_dict.append(split_dictionary)
        return list_of_dict


print("***********Preparing data**************")
for n in range(0, g_EntriesPerChid):
    g_Timestamps.append(startTS + 0.001 * n)

Filename = datetime.datetime.fromtimestamp(g_Timestamps[0]).strftime('%Y_%m_%dT%H_%M_%S') + "-" + \
           datetime.datetime.fromtimestamp(g_Timestamps[len(g_Timestamps) - 1]).strftime('%Y_%m_%dT%H_%M_%S') + ".csv"


with open(Filename, 'w') as outfile:
    i = 0;
    for fieldname in g_row_names_types:
        if i > 0:
            outfile.write(";")
        outfile.write(fieldname)
        i = i + 1
    outfile.write("\n")

with open(Filename, 'a') as outfile:
    for timestamp in g_Timestamps:
        for chid in g_devices:
            TTC_type = ''.join(random.choices(string.ascii_lowercase, k=5))
            TTC = round(random.uniform(0, 1000), 2)
            TET = round(random.uniform(0, 1000), 2)
            TIT = round(random.uniform(0, 1000), 2)
            PET_type = ''.join(random.choices(string.ascii_lowercase, k=5))
            PET_i_1 = random.randint(0, 100)
            PET_i = random.randint(0, 100)
            PET = round(random.uniform(0, 1000), 2)
            Segment_ID = ''.join(random.choices(string.ascii_lowercase, k=5))
            LP = round(random.uniform(0, 1000), 2)
            Lo_AR = round(random.uniform(0, 1000), 2)
            Jerk = round(random.uniform(0, 1000), 2)
            La_AR = round(random.uniform(0, 1000), 2)
            VD = round(random.uniform(0, 1000), 2)
            DRS = round(random.uniform(0, 1000), 2)
            RSS_type = ''.join(random.choices(string.ascii_lowercase, k=5))
            min_lo_distance = round(random.uniform(0, 1000), 2)
            gap_lo_distance = round(random.uniform(0, 1000), 2)
            min_la_distance = round(random.uniform(0, 1000), 2)
            gap_la_distance = round(random.uniform(0, 1000), 2)

            line = ""
            line += str(chid) + ";"
            line += datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.%f') + ";"
            line += TTC_type + ";"
            line += "{:.1f}".format(TTC) + ";"
            line += "{:.1f}".format(TET) + ";"
            line += "{:.1f}".format(TIT) + ";"
            line += PET_type + ";"
            line += str(PET_i_1) + ";"
            line += str(PET_i) + ";"
            line += "{:.1f}".format(PET) + ";"
            line += Segment_ID + ";"
            line += "{:.1f}".format(LP) + ";"
            line += "{:.1f}".format(Lo_AR) + ";"
            line += "{:.1f}".format(Jerk) + ";"
            line += "{:.1f}".format(La_AR) + ";"
            line += "{:.1f}".format(VD) + ";"
            line += "{:.1f}".format(DRS) + ";"
            line += RSS_type + ";"
            line += "{:.1f}".format(min_lo_distance) + ";"
            line += "{:.1f}".format(gap_lo_distance) + ";"
            line += "{:.1f}".format(min_la_distance) + ";"
            line += "{:.1f}".format(gap_la_distance)
            outfile.write(line + "\n")

DictionaryList = make_Dictionary(Filename)

s = 0
e = g_WriteBatchSize
doLoop = True
print("***********Starting write**************")
while doLoop:
    if e > len(DictionaryList):
        doLoop = False
        e = len(DictionaryList)
    if e-s < 1:
        break
    curr_data = DictionaryList[s:e]
    curr_batch = dict();
    curr_batch[g_DataTypeName] = g_DataTypeValue
    curr_batch[g_DataListName] = curr_data
    #json_body_to_send = json.dumps(curr_batch)
    #print(json_body_to_send)
    startT = time.time()
    response = requests.post(g_api_w_url, json=curr_batch)
    endT = time.time()
    #print(response.json())
    print("Write took " + str(endT - startT) + " seconds")
    print("********End of batch*******")
    s = e
    e = e + g_WriteBatchSize

print("***********Starting read**************")

curr_read_body = dict()
curr_read_body["DataType"] = g_DataTypeValue
time_filter = dict()
time_filter["StartTime"] = datetime.datetime.fromtimestamp(g_Timestamps[0] - 5).strftime('%Y-%m-%dT%H:%M:%SZ')
time_filter["EndTime"] = datetime.datetime.fromtimestamp(g_Timestamps[len(g_Timestamps)-1] + 5).strftime('%Y-%m-%dT%H:%M:%SZ')
curr_read_body["TimeFilter"] = time_filter
print(curr_read_body)
startT = time.time()
response = requests.post(g_api_r_url, json=curr_read_body)
endT = time.time()
print("Read took " + str(endT - startT) + " seconds")
#print(response.json()["Response"])
chiddata = dict()
tsdata = dict()
out_elemets = list()
for elem in response.json()["Response"]:
    json_object = json.loads(elem["Json"])
    #print(json_object)
    chiddata["Chid"] = elem["Chid"]
    tsdata["TimeStamp"] = elem["Time"]
    resdata = {**chiddata, **tsdata}
    resdata = {**resdata, **json_object}
    out_elemets.append(resdata)
    #print(resdata)

print(len(out_elemets))

out_elemets.sort(key= lambda x: (x["TimeStamp"],x["Chid"]))


keys = out_elemets[0].keys()

with open("out_" + Filename, 'w', newline='\n') as output_file:
    dict_writer = csv.DictWriter(output_file, keys, delimiter=';')
    dict_writer.writeheader()
    dict_writer.writerows(out_elemets)
