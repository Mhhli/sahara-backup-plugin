
import json
import os
import shutil


def join_path(path, file_name):
    return os.path.join(path, file_name)


def is_path_exist(path):
    return os.path.exists(path)


def create_dir(path):
    if is_path_exist(path):
        delete_dir(path)
    os.makedirs(path)


def delete_dir(path):
    if is_path_exist(path):
        shutil.rmtree(path)


def save_to_json(path, data):
    with open(path, 'w') as save_json:
        json.dump(data, save_json, ensure_ascii=False)


def read_from_json(path):
    with open(path, 'r') as load_json:
        data = json.load(load_json)
    return data


def append_to_list(element, _list):
    if element not in _list:
        _list.append(element)
    return _list


def get_rand_char():
    pass
