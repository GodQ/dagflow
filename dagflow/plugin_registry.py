__author__ = 'godq'
import sys
import os
import logging

from dagflow.exceptions import PlugInHasExisted
from dagflow.event_center.base_request_filter import RequestFilter

logger = logging.getLogger('plugin-registry')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PLUGINS = dict()


def add_plugin(name, func):
    if name not in PLUGINS:
        PLUGINS[name] = func
    # else:
    #     raise PlugInHasExisted("Plugin {}:{} has been existent!".format(k, v))


def get_plugin(name):
    func = PLUGINS.get(name)
    if not func:
        load_plugins()
        func = PLUGINS.get(name)
    return func


def __load_plugin_dict(plugin_conf):
    for k, v in plugin_conf.items():
        # if k in PLUGINS:
        #     raise PlugInHasExisted("Plugin {}:{} has been existent!".format(k, v))
        add_plugin(k, v)
        logger.info("Load plugin {}".format(k))


def auto_load_plugins(plugin_dir=os.path.join(BASE_DIR, "plugins")):
    print(plugin_dir)
    if not os.path.isdir(plugin_dir):
        return
    if plugin_dir not in sys.path:
        sys.path.append(plugin_dir)
    for filename in os.listdir(plugin_dir):
        path = os.path.join(plugin_dir, filename)
        if os.path.isfile(path) and filename.endswith(".py") and \
                filename != "__init__.py":
            module_name = filename[:-3]
            try:
                print(module_name)
                module = __import__(module_name)
                if hasattr(module, "PLUGINS"):
                    plugin_conf = module.PLUGINS
                    __load_plugin_dict(plugin_conf)
                if hasattr(module, "FILTERS"):
                    filter_list = module.FILTERS
                    RequestFilter.add_filters(filter_list)
            except ImportError as e:
                logger.error(str(e))
    print()


def load_plugins():
    # auto load built-in plugins and user plugins
    from dagflow.plugins.plugin_demo import PLUGINS as plugin_conf
    __load_plugin_dict(plugin_conf)
    from dagflow.plugins.resource_filter import FILTERS as filter_list
    RequestFilter.add_filters(filter_list)

    # auto_load_plugins()
    user_plugin_dir = os.environ.get("USER_PLUGINS_PATH", None)
    if user_plugin_dir:
        auto_load_plugins(user_plugin_dir)


load_plugins()
