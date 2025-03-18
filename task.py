#!/usr/bin/env python

from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("shortest_path").setMaster("yarn")
sc = SparkContext(conf=config)

edges_path = "/data/twitter/twitter_sample.txt"
start_vertex = 12
end_vertex = 34

def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))

def step(item):
    prev_v, prev_d, next_v = item[0], item[1][0], item[1][1]
    return (next_v, prev_d + 1)

def complete(item):
    v, old_d, new_d = item[0], item[1][0], item[1][1]
    return (v, old_d if old_d is not None else new_d)

def path_step(item):
    last_vertex, path, new_vertex = item[0], item[1][0], item[1][1]
    last_vertex = new_vertex
    path += tuple([new_vertex])
    return (last_vertex, path)

num_partitions = 400
edges = sc.textFile(edges_path, num_partitions).map(parse_edge).distinct()

def shortest_path(sc, edges, start_vertex, end_vertex):
    num_partitions = edges.getNumPartitions()
    forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(num_partitions)

    paths = sc.parallelize([(start_vertex, tuple([start_vertex]))]).partitionBy(num_partitions)

    x = start_vertex
    d = 0

    while True:
        d += 1
        paths = paths.join(forward_edges, num_partitions).map(path_step)

        if paths.filter(lambda x: x[0] == end_vertex).take(1):
            break

    min_path = paths.filter(lambda x: x[0] == end_vertex).values().first()
    return min_path

min_path = shortest_path(sc, edges, start_vertex, end_vertex)
if min_path:
    print(",".join(map(str, min_path)))
else:
    print("Path not found")

