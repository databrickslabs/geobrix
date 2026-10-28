"""A Spark Connect look-alike for host tests: the module path contains "connect", which is
what `capabilities.is_connect` keys on. It records forbidden attribute access instead of serving it.
"""


class _Udf:
    def __init__(self, owner):
        self.owner = owner

    def register(self, name, f, returnType=None):
        self.owner.registered.append(name)

    def registerJavaFunction(self, name, cls, returnType=None):
        raise RuntimeError("[CANNOT_LOAD_FUNCTION_CLASS] class not found")


class ConnectStub:
    def __init__(self):
        self.touched, self.registered = [], []
        self.udf = _Udf(self)

    def __getattr__(self, name):
        if name in ("_jvm", "_jsc", "sparkContext", "_jsparkSession", "conf"):
            self.__dict__.setdefault("touched", []).append(name)
            raise AssertionError(f"forbidden on Connect: {name}")
        raise AttributeError(name)
