__author__ = 'godq'


def echo(args):
    name = args.get("name")
    print(name)
    return name


PLUGINS = {
    "echo": echo,
}
