from inspect import isclass
import os
import sys
from numpy.core import ufunc
import inspect
from io import StringIO

CACHE = {}


def get_member(name: str, n: str, v: object) -> str:
    try:
        item_name = getattr(v, "__name__", "%s.%s" % (name, n))
        mod_name = getattr(v, "__module__", None)
    except NameError:
        item_name = "%s.%s" % (name, n)
        mod_name = None
    return item_name, mod_name


def is_exe(
    item_name: str, mod_name: str, name: str, n: str, v: object, _all: str
) -> bool:
    if "." not in item_name and mod_name:
        item_name = "%s.%s" % (mod_name, item_name)

    if not item_name.startswith(name + "."):
        if isinstance(v, ufunc):
            return False
        else:
            return True
    elif not (inspect.ismodule(v) or _all is None or n in _all):
        return True


def get_docs(name: str, kind: str, index: int, item: object) -> None:
    try:
        doc = inspect.getdoc(item)
    except NameError as e:
        raise NameError(title="Name Error", detail=e)

    if doc is not None:
        CACHE[name] = (doc, kind, index)


def path_to_import(this_py: str, mod_path: str, init_py: str) -> str:
    if os.path.isfile(this_py) and mod_path.endswith(".py"):
        return mod_path[:-3]
    elif os.path.isfile(init_py):
        return mod_path
    else:
        return None


def get_module(module: str) -> object:
    try:
        __import__(module)
    except ImportError:
        return {}

    return sys.modules[module]


def get_all(item: object) -> object:
    try:
        _all = item.__all__
    except AttributeError:
        _all = None
    return _all


def _import(name: str, to_import: str) -> bool:
    try:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            sys.stdout = StringIO()
            sys.stderr = StringIO()
            __import__("%s.%s" % (name, to_import))
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return True
    except (Exception, SystemExit):
        return False


def push_next_members(
    import_modules: bool, item: object, name: str, _all: object, stack
) -> None:
    if import_modules and hasattr(item, "__path__"):
        for pth in item.__path__:
            for mod_path in os.listdir(pth):
                this_py = os.path.join(pth, mod_path)
                init_py = os.path.join(pth, mod_path, "__init__.py")
                to_import = path_to_import(this_py, mod_path, init_py)
                if not to_import or to_import == "__init__":
                    continue
                if not _import(name, to_import):
                    continue

    for n, v in _getmembers(item):
        item_name, mod_name = get_member(name, n, v)
        if is_exe(item_name, mod_name, name, n, v, _all):
            continue
        stack.append(("%s.%s" % (name, n), v))


def _lookfor_generate_cache(module: str, import_modules: bool) -> dict:
    module = get_module(module)
    seen = {}
    index = 0
    stack = [(module.__name__, module)]

    while stack:
        name, item = stack.pop(0)
        if id(item) in seen:
            continue
        seen[id(item)] = True

        index += 1
        kind = "object"

        if inspect.ismodule(item):
            kind = "module"
            _all = get_all(item)
            push_next_members(import_modules, item, name, _all, stack)
        elif inspect.isclass(item):
            kind = "class"
            for n, v in _getmembers(item):
                stack.append(("%s.%s" % (name, n), v))
        elif hasattr(item, "__call__"):
            kind = "func"
        get_docs(name, kind, index, item)
    return CACHE


def _getmembers(item: object) -> object:
    import inspect

    try:
        members = inspect.getmembers(item)
    except Exception:
        members = [(x, getattr(item, x)) for x in dir(item) if hasattr(item, x)]
    return members


def lookfor(module: str = None, import_modules: bool = True) -> list:

    CACHE = _lookfor_generate_cache(module, import_modules)

    found = []

    for name, (docstring, kind, index) in CACHE.items():
        if kind in ("module", "object"):
            continue
        doc = docstring.lower()
        found.append((name, kind, doc))

    return found


def checkclass(cls) -> None:
    if not isclass(cls):
        print("Note : use class to get help")
        raise TypeError(title="Use class")

    if cls.__name__ not in ["Polly", "OmixAtlas", "Cohort", "Workspaces"]:
        print("Other class methods not allowed")
        raise Exception(title="Other class are not allowed")


def get_fun_name(function_name: str) -> str:
    if function_name and function_name.find("(") != -1:
        return function_name.split("(")[0]
    elif function_name:
        return function_name
    return None


def checkpath(fun: str) -> bool:
    should_show_fun = True
    for name in fun.split("."):
        if name.startswith("_"):
            should_show_fun = False
            break
    return should_show_fun


def get_line(fun: str, kind: str, txt: str, function_name: str, cls, doc: str) -> str:
    Link = {
        "cohort": "https://github.com/ElucidataInc/PublicAssets/blob/master/polly-python/example/cohort.ipynb",
        "omixatlas": "https://github.com/ElucidataInc/PublicAssets/blob/master/polly-python/example/omixatlas.ipynb",
        "polly": "https://github.com/ElucidataInc/PublicAssets/blob/master/polly-python/example/polly.ipynb",
        "workspaces": "https://github.com/ElucidataInc/PublicAssets/blob/master/polly-python/example/workspaces.ipynb",
    }
    line = None
    class_name = cls.__name__.lower()

    description = txt.split("\n")[0]
    if doc:
        if kind == "class" or function_name:
            line = (
                "Name - %s() \nDescription - %s\nDocstring - %s\n%s class contain following methods\n\n"
                % (fun.split(".")[-1], description, txt, cls.__name__)
            )
        else:
            line = "Name - %s() \n   Description - %s\n" % (
                fun.split(".")[-1],
                description,
            )
    else:
        if kind == "class" and not function_name:
            line = "Name - %s() \n   Description - %s\n   Link - %s\n" % (
                fun.split(".")[-1],
                description,
                Link[class_name],
            )

        elif kind == "func" and function_name:
            line = "Name - %s() \n   Description - %s\n   Link - %s\n" % (
                fun.split(".")[-1],
                description,
                Link[class_name],
            )
    return line


def get_txt(
    cls,
    fun: str,
    kind: str,
    doc: str,
    function_name: str,
    should_show_fun: bool,
    is_doc: bool,
) -> str:
    if (
        cls.__name__ in fun.split(".")
        and (not function_name or function_name == fun.split(".")[-1])
        and should_show_fun
    ):
        import rst2txt
        from docutils.core import publish_string

        txt = publish_string(
            source=doc,
            writer=rst2txt.Writer(),
            settings_overrides={"report_level": 6},
        ).decode("utf-8")

        while txt.find(">>:ref:`") != -1:
            x = txt.find(">>:ref:`")
            y = txt.find("`<<") + 3
            str_to_del = txt[x:y]
            str_to_ins = str_to_del.split(">>:ref:`")[1]
            str_to_ins = "visit " + str_to_ins.split(" ")[0]
            txt = txt.replace(str_to_del, str_to_ins)

        if "meta private" not in txt:
            line = get_line(fun, kind, txt, function_name, cls, is_doc)
            return line
    return None


def doc(cls, function_name: str = "") -> None:
    """
    function to see documentation for class - Polly, OmixAtlas, Workspaces, Cohort and it's member funtions

    ``Args:``
        ``function_name (optional) str:`` provide function name to see documentation default empty.

    ``Returns:``
        if you provide function_name to search then it will give documentation for function other wise it will\
 give documentation for the class.
    """
    checkclass(cls)
    function_name = get_fun_name(function_name)
    help_text = []

    for (fun, kind, doc) in lookfor("polly", True):
        should_show_fun = checkpath(fun)
        txt = get_txt(cls, fun, kind, doc, function_name, should_show_fun, True)
        if txt:
            help_text.append(txt)
    if len(help_text) == 0:
        help_text.append("Nothing found")
    help_text.append(
        "Go to our polly documentation https://vito39.github.io/testing_repo_2/"
    )

    print("\n".join(help_text))


def example(cls, function_name: str = "") -> None:
    """
    function to see examples for class - Polly, OmixAtlas, Workspaces, Cohort and it's member funtions

    ``Args:``
        ``function_name (optional) str:`` provide function name to see examples default empty.

    ``Returns:``
        if you provide function_name to search then it will give example link for that function other wise it \
will give link for all examples for that class.
    """
    checkclass(cls)
    function_name = get_fun_name(function_name)
    help_text = []

    for (fun, kind, doc) in lookfor("polly", True):
        should_show_fun = checkpath(fun)
        txt = get_txt(cls, fun, kind, doc, function_name, should_show_fun, False)
        if txt:
            help_text.append(txt)

    if help_text == []:
        help_text.append("nothing found")

    print("\n".join(help_text))
