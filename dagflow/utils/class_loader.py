__author__ = 'godq'
import importlib
import sys
import os

root_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, "executors"))
sys.path.append(os.path.join(root_dir, "dag_repos"))
sys.path.append(os.path.join(root_dir, "plugins"))


class ClassLoader:
    @staticmethod
    def load(class_path):
        try:
            class_obj = importlib.import_module(class_path)
        except:
            t = class_path.split('.')
            # module_path = '.'.join(t[:-1])
            module_path = t[-2]
            module_obj = importlib.import_module(module_path)
            class_obj = getattr(module_obj, t[-1])
        return class_obj


if __name__ == '__main__':
    cls = ClassLoader.load("dagflow.executors.celery_executor.CeleryExecutor")
    print(dir(cls))