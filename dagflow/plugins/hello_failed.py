__author__ = 'godq'


def hello_plugin_failed(args):
    name = args.get("name")
    print(name)
    assert 1 == 0
    return name


PLUGINS = {
    "hello_plugin_failed": hello_plugin_failed,
}
