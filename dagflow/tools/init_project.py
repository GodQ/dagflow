__author__ = 'godq'
import os
import shutil


def create_project(pro_path):
    if os.path.exists(pro_path):
        raise Exception("Path {} has existed".format(pro_path))

    empty_project_path = os.path.join(os.path.dirname(__file__), "empty_project")
    shutil.copytree(empty_project_path, pro_path)
    return pro_path
