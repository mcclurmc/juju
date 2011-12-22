
_marker = object()


def _get_module_function(specification):
    # converts foo.bar.baz to ['foo.bar', 'baz']
    try:
        data = specification.rsplit('.', 1)
    except (ValueError, AttributeError):
        data = []

    if len(data) != 2:
        raise ValueError("Invalid import specification: %r" % (
                specification))
    return data


def load_symbol(specification):
    """load a symbol from a dot delimited path in the import
    namespace.
    returns (module, symbol) or raises ImportError
    """
    module_path, symbol_name = _get_module_function(specification)
    module = __import__(module_path, fromlist=module_path.split())
    symbol = getattr(module, symbol_name, _marker)
    return (module, symbol)


def get_callable(specification):
    """
    Convert a string version of a function name to the callable
    object. If no callable can be found an ImportError is raised.
    """
    module, callback = load_symbol(specification)
    if callback is _marker or not callable(callback):
        raise ImportError("No callback found for %s" % (
                specification))
    return callback
