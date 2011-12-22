import os
import sys

INIT = "__init__.py"
TESTS = "tests"


def get_modules(names):
    if INIT in names:
        names.remove(INIT)
    return [n[:-3] for n in names if n.endswith(".py")]


def trim_dirs(root, dirs):
    for dir_ in dirs[:]:
        if dir_ == TESTS:
            dirs.remove(TESTS)
        if not os.path.exists(os.path.join(root, dir_, INIT)):
            dirs.remove(dir_)
    return dirs


def module_name(base, root, name=None):
    path = root[len(base) + 1:]
    if name:
        path = os.path.join(path, name)
    return path.replace("/", ".")


def collect_modules(src):
    src = os.path.abspath(src)
    base = os.path.dirname(src)

    names = []
    for root, dirs, files in os.walk(src):
        modules = get_modules(files)
        packages = trim_dirs(root, dirs)
        if modules or packages:
            names.append(module_name(base, root))
            for name in modules:
                names.append(module_name(base, root, name))
    return sorted(names)


def subpackages(names, parent):
    return [name for name in names
            if name.startswith(parent) and name != parent]


def dst_file(dst, name):
    return open(os.path.join(dst, "%s.rst" % name), "w")


def write_title(f, name, kind):
    f.write("%s\n%s\n\n" % (name, kind * len(name)))


def write_packages(f, names):
    for name in names:
        f.write("    " * name.count("."))
        f.write("* :mod:`%s`\n" % name)
    f.write("\n")


def abbreviate(name):
    parts = name.split(".")
    short_parts = [part[0] for part in parts[:-2]]
    return ".".join(short_parts + parts[-2:])


def write_module(f, name, subs):
    write_title(f, abbreviate(name), "=")
    f.write(".. automodule:: %s\n"
            "    :members:\n"
            "    :undoc-members:\n"
            "    :show-inheritance:\n\n"
            % name)
    if subs:
        write_title(f, "Subpackages", "-")
        write_packages(f, subs)


def write_module_list(f, names):
    write_title(f, "juju modules", "=")
    write_packages(f, names)
    f.write(".. toctree::\n    :hidden:\n\n")
    for name in names:
        f.write("    %s\n" % name)


def generate(src, dst):
    names = collect_modules(src)

    if not os.path.exists(dst):
        os.makedirs(dst)

    with dst_file(dst, "modules") as f:
        write_module_list(f, names)

    for name in names:
        with dst_file(dst, name) as f:
            write_module(f, name, subpackages(names, name))


if __name__ == "__main__":
    src, dst = sys.argv[1:]
    generate(src, dst)
