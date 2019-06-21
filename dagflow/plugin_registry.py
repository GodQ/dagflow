__author__ = 'godq'
import sys
import os
import logging

from dagflow.exceptions import PlugInHasExisted

logger = logging.getLogger('plugin-registry')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


PLUGINS = dict()


def auto_load_tasks(task_dir=os.path.join(BASE_DIR, "plugins")):
    sys.path.append(task_dir)
    for filename in os.listdir(task_dir):
        path = os.path.join(task_dir, filename)
        if os.path.isfile(path) and filename.endswith(".py") and \
                filename != "__init__.py":
            module_name = filename.strip(".py")
            try:
                module = __import__(module_name)
                plugin_conf = module.PLUGINS
                for k, v in plugin_conf.items():
                    if k in PLUGINS:
                        raise PlugInHasExisted("Plugin {}:{} has been existent!".format(module_name, k))
                    PLUGINS[k] = v
                    logger.info("Load plugin {}:{}".format(module_name, k))
            except ImportError as e:
                logger.error(str(e))
    print()


auto_load_tasks()


def get_plugin(name):
    return PLUGINS.get(name)

