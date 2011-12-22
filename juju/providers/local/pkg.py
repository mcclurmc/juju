import apt


def check_packages(*packages):
    """Given a list of packages, return the packages which are not installed.
    """
    cache = apt.Cache()
    missing = set()
    for pkg_name in packages:
        try:
            pkg = cache[pkg_name]
        except KeyError:
            missing.add(pkg_name)
            continue
        if pkg.is_installed:
            continue
        missing.add(pkg_name)
    return missing
