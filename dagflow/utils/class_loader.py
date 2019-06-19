__author__ = 'godq'
import importlib


class ClassLoader:
    @staticmethod
    def load(class_path):
        try:
            class_obj = importlib.import_module(class_path)
        except ModuleNotFoundError:
            t = class_path.split('.')
            module_path = '.'.join(t[:-1])
            print(t, module_path)
            module_obj = importlib.import_module(module_path)
            class_obj = getattr(module_obj, t[-1])
        print(class_obj)
        return class_obj


if __name__ == '__main__':
    cls = ClassLoader.load("dagflow.executors.celery_executor.CeleryExecutor")
    print(dir(cls))