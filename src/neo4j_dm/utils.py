import os
import pathlib
import shutil
import operator
import functools

import neo4j
import momapy_kb.neo4j.core


def remake_dir(dir_path):
    if pathlib.Path(dir_path).exists():
        shutil.rmtree(dir_path)
    os.makedirs(dir_path, exist_ok=True)


def rename_file_extension(file_name, new_extension):
    pre, ext = os.path.splitext(file_name)
    return f"{pre}.{new_extension}"


def get_file_name_from_file_path(file_path):
    return os.path.basename(file_path)


def flatten_list(input_list):
    def _flatten_rec(a, b):
        if isinstance(b, list):
            b = flatten_list(b)
        else:
            b = [b]
        return operator.iconcat(a, b)

    return functools.reduce(_flatten_rec, input_list, [])


def get_nodes_and_relationships_from_query_result(result):
    nodes = []
    relationships = []
    for result_element in flatten_list(result):
        if isinstance(result_element, neo4j.graph.Node):
            nodes.append(result_element)
        elif isinstance(result_element, neo4j.graph.Relationship):
            relationships.append(result_element)
    return nodes, relationships


def get_number_and_size_of_clusters(list_of_sets):
    i = 0
    while i < len(list_of_sets):
        j = i + 1
        while j < len(list_of_sets):
            if list_of_sets[i].intersection(list_of_sets[j]):
                list_of_sets[i] = list_of_sets[i].union(list_of_sets[j])
                del list_of_sets[j]
                j = i + 1
            else:
                j += 1
        i += 1
    return len(list_of_sets), [len(set_) for set_ in list_of_sets]


def check_connection():
    if not momapy_kb.neo4j.core.is_connected():
        raise ConnectionError("you must connect to the database first")


class ConnectionError(Exception):
    pass
