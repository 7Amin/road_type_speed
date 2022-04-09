import pandas as pd
import requests
import argparse 
import os
import math
from xml.etree.ElementTree import fromstring, ElementTree
from datetime import datetime
import json
import statistics 
from collections import Counter


TrafficEvents = None

chunk_size = 1000
max_distance_from_node = 50  # Meter
number_of_nearest_node = 5  # count
name_folder = "0"
index_start_point_file = 0
index_end_point_file = 499

OSM_BASIC_URL = "https://api.openstreetmap.org/api/0.6/"
BASE_PATH = "../data/speed/"
OUT_PATH = "type/"

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("-c", '--chunk_size', type=int,  default=1000, help='chunk size')
parser.add_argument("-md", "--max_distance", type=int, default=50, help="max distance from node")
parser.add_argument("-nn", "--nearest_node", type=int, default=5, help="number of nearest node")
parser.add_argument("-nf", "--name_folder", type=str, default='0', help="name folder")
parser.add_argument("-is", "--index_start_point_file", type=int, default=0, help="index start point file")
parser.add_argument("-ie", "--index_end_point_file", type=int, default=499, help="index_end_point_file")
args = parser.parse_args()

chunk_size = args.chunk_size
max_distance_from_node = args.max_distance
number_of_nearest_node = args.nearest_node
name_folder = args.name_folder
index_start_point_file = args.index_start_point_file
index_end_point_file = args.index_end_point_file


class OSM(object):
    @staticmethod
    def get_node_details_by_node_id(node_id):
        URL = OSM_BASIC_URL + "node/{}".format(node_id)
        response = requests.get(url = URL)
        result_tree = ElementTree.fromstring(response.content)
        return result_tree, response.content
    
    @staticmethod
    def get_all_ways_contain_node_by_node_id(node_id):
        URL = OSM_BASIC_URL + "node/{}/ways".format(node_id)
        # print(URL)
        response = requests.get(url = URL)
        # result_tree = ElementTree.fromstring(response.content)
        result_tree = ElementTree(fromstring(response.content))

        return result_tree, response.content
    
    @staticmethod
    def get_type_of_way_contain_node_by_node_id(node_id):
        way_type = "None" 
        if node_id == 0:
            return way_type
        try:
            result_tree, content = OSM.get_all_ways_contain_node_by_node_id(node_id)
            root = result_tree.getroot()
            for child in root:
                if child.tag == "way":
                    for child1 in child:
                        if child1.tag == "tag" and child1.attrib["k"] == "highway":
                            way_type = child1.attrib["v"]
                            return way_type
        except:
            print("ERROR: node id {} has error".format(node_id))
            
        return way_type


def read_data(path):
    print("read csv data is started with file {}".format(path))
    data = pd.read_csv(path)
    print("reading is finished length is {} ".format(len(data)))
    return data


def most_frequent(List):
    data = []
    for l in List:
        if not(l == "None"):
            data.append(l)
    occurence_count = Counter(data)
    if len(data) == 0:
        return "None"
    return occurence_count.most_common(1)[0][0]


out_dir = BASE_PATH + OUT_PATH + str(name_folder)
print(f"out : {out_dir}")

if not os.path.exists(out_dir):
     os.mkdir(out_dir)

for i in range(index_start_point_file, index_end_point_file):
    type_of_ways_all_points = []
    print("file id is {}".format(i))
    print("start time is {}".format(datetime.now()))
    path = BASE_PATH + str(name_folder) + "/" + "file_" + str(i) + ".csv"
    
    traffic_events_data = read_data(path)
    for index, data in traffic_events_data.iterrows():
        start_points_node_id = json.loads(data['nearst_nodes_ids_of_start_point'])
        end_points_node_id = json.loads(data['nearst_nodes_ids_of_end_point'])
        
        start_points_node_id = start_points_node_id[:3]
        end_points_node_id = end_points_node_id[:2]
        
        ways_id = set(start_points_node_id + end_points_node_id)
        type_of_ways = []
        while len(ways_id) > 0:
            node_id = ways_id.pop()
            type_of_way = OSM.get_type_of_way_contain_node_by_node_id(node_id)
            type_of_ways.append(type_of_way)
        type_of_road = most_frequent(type_of_ways)
        if index % 50 == 0:
            print("number: {} is {}".format(index, type_of_road))
            print("Time is {}".format(datetime.now()))
        type_of_ways_all_points.append(type_of_road)
    
    traffic_events_data["type_of_roads"] = type_of_ways_all_points
    traffic_events_data = traffic_events_data.drop("nearst_nodes_ids_of_start_point", axis=1)
    traffic_events_data = traffic_events_data.drop("nearst_nodes_ids_of_end_point", axis=1)
    traffic_events_data = traffic_events_data.drop("nearst_nodes_of_end_point", axis=1)
    traffic_events_data = traffic_events_data.drop("nearst_nodes_of_start_point", axis=1)
    
    output_file_url = out_dir + "/" + "file_" + str(i) + ".csv"
    traffic_events_data.to_csv (output_file_url, index=False, header=True)
    print(traffic_events_data.columns)
    print("{} done".format(output_file_url))
    print("end time is {}".format(datetime.now()))
