import logging
import os
import sys
import json
import importlib

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger('dagflow')


class RequestFilterStatus:
    PASS = 0
    DELAY = 1
    FORBID = 2

    @classmethod
    def to_str(cls, status):
        for k, v in cls.__dict__.items():
            if isinstance(v, int) and v == status:
                return k


class RequestFilter:
    filter_list = list()

    @classmethod
    def add_filter(cls, func):
        if func not in cls.filter_list:
            cls.filter_list.append(func)

    @classmethod
    def run_filter(cls, request):
        assert isinstance(request, dict)

        status = RequestFilterStatus.PASS
        massage = None
        for filter_func in cls.filter_list:
            st, msg = filter_func(request)
            logger.info("Filter {}.{} result: {}".format(
                filter_func.__module__, filter_func.__name__, RequestFilterStatus.to_str(st)))
            if st > status:
                status = st
                massage = msg
        return status, massage


def auto_load_filter():
    filter_dir = BASE_DIR
    if filter_dir not in sys.path:
        sys.path.append(filter_dir)
    for fname in os.listdir(filter_dir):
        path = os.path.join(filter_dir, fname)
        if os.path.isfile(path) and fname.endswith("_filter.py") and \
                path != __file__:
            module_name = fname[:-3]
            try:
                logger.warning("load filter module {}".format(module_name))
                # __import__(module_name)
                module = importlib.import_module(module_name)
                if "FILTERS" in module.__dict__ and module.FILTERS:
                    for f in module.FILTERS:
                        RequestFilter.add_filter(f)
            except ImportError as e:
                logger.error(str(e))


# auto_load_filter()