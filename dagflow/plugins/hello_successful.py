__author__ = 'godq'


def hello1(args):
    print("Hello")
    return "Hello"


def hello2(args):
    name = args.get("name")
    print(name)
    return name


PLUGINS = {
    "hello_plugin1": hello1,
    "hello_plugin2": hello2,
}
