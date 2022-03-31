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

chunk_size = 10 ** 3
max_distance_from_node = 50 # Meter
number_of_nearest_node = 5 # count
name_folder = "0"
index_start_point_file = 0
index_end_point_file = 499

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

OSRM_BASIC_URL = "https://usaeta.bluebitsoft.com/"
BASE_PATH = "../data/"
OUT_PATH = "type/"
 


def read_big_data_by_filter_with_key_values(path, filters):
    data = None
    print("read csv data is started with file {}".format(path))
    for num, df in enumerate(pd.read_csv(path, chunksize=chunk_size), start=1):
        print("continue reading file page num is {}".format(num))
        data = df.append(data)
    print("method {} finished ".format("read_big_data_filter_with_key_values"))
    return data


class Location(object):
    def __init__(self, osrm_data):
        self.longitude = osrm_data[0]
        self.latitude = osrm_data[1]


class WayPoint(object):
    def __init__(self, json_data):
        self.nodes = json_data["nodes"]
        self.hint = json_data["hint"]
        self.distance = json_data["distance"]
        self.name = json_data["name"]
        self.location = Location(json_data["location"])
    
    def validate_way_point(self):
        if self.distance < max_distance_from_node:
            return True
        return False


class OSRM(object):
    @staticmethod
    def get_all_nearest_nodes_of_location(location, number=1):
        params = {
            "number": number
        }
        URL = OSRM_BASIC_URL + "nearest/v1/driving/{},{}".format(location.longitude, location.latitude)
        print(URL)
        response = requests.get(url = URL, params = params)
        return response.json()


    def get_eta_and_distance(origin, destination):
        try:
            URL = OSRM_BASIC_URL + "route/v1/driving/{},{};{},{}".format(
                origin.longitude, origin.latitude, destination.longitude, destination.latitude)
            print(URL)
            response = requests.get(url = URL)
            result = response.json()
            routes = result["routes"]
            route = routes[0]
            eta = route["duration"]
            distance = route["distance"]
            speed = distance / eta
        except:
            eta = -100
            distance = -100
            speed = -100
        return eta, distance, speed



dir = os.path.join(BASE_PATH, name_folder)
out_dir = os.path.join(BASE_PATH, "speed", name_folder)
print(dir)
print(out_dir)
if not os.path.exists(dir):
    print("does not exists {}".format(dir))
    raise("does not exists")
    
if not os.path.exists(out_dir):
	os.mkdir(out_dir)


for file_index in range(index_start_point_file, index_end_point_file):
    print("file_index: {}".format(file_index))
    file_url = dir + "/file_{}.csv".format(file_index)

    
    traffic_events_data = read_big_data_by_filter_with_key_values(file_url, traffic_filters)
    speeds = []
	distances = []
	etas = []
    nearst_nodes_of_start_point = []
    nearst_nodes_of_end_point = []
    nearst_node_ids_of_start_validate_point = []
    nearst_node_ids_of_end_validate_point = []
    print("start file in {}".format(datetime.now()))
    for index, data in traffic_events_data.iterrows():
    	speed = -100
	    distance = -100
	    eta = -100
        start_location = Location([data["Start_Lng"], data["Start_Lat"]])
        if "EndPoint_Lng" in data and "End_Lat" in data and not math.isnan(data["End_Lat"]):
            end_location = Location([data["End_Lng"], data["End_Lat"]])
            eta, distance, speed = OSRM.get_eta_and_distance(start_location, end_location)
        else:
            end_location = Location([data["Start_Lng"], data["Start_Lat"]])

        nearst_nodes_start_location = OSRM.get_all_nearest_nodes_of_location(start_location, number_of_nearest_node)
        nearst_nodes_end_location = OSRM.get_all_nearest_nodes_of_location(end_location, number_of_nearest_node)
    
    
        nearst_way_points_start_location = []
        start_ids = []
        for point in nearst_nodes_start_location["waypoints"]:
            way_point = WayPoint(point)
            if way_point.validate_way_point():
                nearst_way_points_start_location.append(way_point)
                start_ids.extend(way_point.nodes)

        nearst_way_points_end_location = []
        end_ids = []
        for point in nearst_nodes_end_location["waypoints"]:
            way_point = WayPoint(point)
            if way_point.validate_way_point():
                nearst_way_points_end_location.append(way_point)
                end_ids.extend(way_point.nodes)
            

        nearst_nodes_of_start_point.append(nearst_nodes_start_location)
        nearst_nodes_of_end_point.append(nearst_nodes_end_location)

        nearst_node_ids_of_start_validate_point.append(start_ids)
        nearst_node_ids_of_end_validate_point.append(end_ids)

        speeds.append(speed)
        etas.append(eta)
        distances.append(distance)
    
        if len(nearst_node_ids_of_start_validate_point) % 100 == 99:
            print(datetime.now())
            print(len(nearst_nodes_of_start_point))
            print("=======")

    traffic_events_data["nearst_nodes_of_start_point"] = nearst_nodes_of_start_point
    traffic_events_data["nearst_nodes_of_end_point"] = nearst_nodes_of_end_point
    traffic_events_data["nearst_nodes_ids_of_start_point"] = nearst_node_ids_of_start_validate_point
    traffic_events_data["nearst_nodes_ids_of_end_point"] = nearst_node_ids_of_end_validate_point
    traffic_events_data["avg_speed"] = speeds
    traffic_events_data["distance"] = distances
    traffic_events_data["eta"] = etas

    output_file_url = out_dir + "/file_{}.csv".format(file_index)
    traffic_events_data.to_csv (output_file_url, index = False, header=True)
    print(traffic_events_data.columns)
    print("{} done".format(output_file_url))
    print("end time is {}".format(datetime.now()))
        
print("end")
