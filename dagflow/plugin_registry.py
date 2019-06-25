__author__ = 'godq'
import sys
import os
import logging

from dagflow.exceptions import PlugInHasExisted

logger = logging.getLogger('plugin-registry')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


PLUGINS = dict()


def auto_load_plugins(plugin_dir=os.path.join(BASE_DIR, "plugins")):
    print(plugin_dir)
    if plugin_dir not in sys.path:
        sys.path.append(plugin_dir)
    for filename in os.listdir(plugin_dir):
        path = os.path.join(plugin_dir, filename)
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


def get_plugin(name):
    func = PLUGINS.get(name)
    if not func:
        load_plugins()
        func = PLUGINS.get(name)
    return func


def load_plugins():
    # auto load built-in plugins and user plugins
    auto_load_plugins()
    user_plugin_dir = os.environ.get("USER_PLUGINS_PATH", None)
    if user_plugin_dir:
        auto_load_plugins(user_plugin_dir)


load_plugins()
