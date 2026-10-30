"""A Spark Connect look-alike for host tests: the module path contains "connect", which is
what `capabilities.is_connect` keys on. It records forbidden attribute access instead of serving it.
"""


class _Udf:
    def __init__(self, owner):
        self.owner = owner

    def register(self, name, f, returnType=None):
        self.owner.registered.append(name)

    def registerJavaFunction(self, name, cls, returnType=None):
        # The standard-access sandbox only sees classes of a JAR added to the SESSION, and only Java ones
        # (`sandbox_classes`); a cluster-library JAR, or a Scala class, is a CLASS_NOT_FOUND at execution.
        owner = self.owner
        if not owner.artifacts or cls not in owner.sandbox_classes:
            raise RuntimeError("[CANNOT_LOAD_FUNCTION_CLASS] class not found")
        owner.java_registered.append((name, cls, returnType))


class _Udtf:
    def __init__(self, owner):
        self.owner = owner

    def register(self, name, f):
        self.owner.registered.append(name)


class ConnectStub:
    def __init__(self):
        self.touched, self.registered = [], []
        self.artifacts, self.java_registered, self.sandbox_classes = [], [], set()
        self.udf = _Udf(self)
        self.udtf = _Udtf(self)

    def sql(self, text):
        # the probes the manager runs once its Java handlers are registered
        from databricks.labs.gbx import __version__

        class _Row:
            def first(_self):
                return {"i": f"{__version__}|abc", "g": "ok:3.13.3"}

        return _Row()

    def addArtifact(self, *path, **kw):
        self.artifacts.extend(path)

    def __getattr__(self, name):
        if name in ("_jvm", "_jsc", "sparkContext", "_jsparkSession", "conf"):
            self.__dict__.setdefault("touched", []).append(name)
            raise AssertionError(f"forbidden on Connect: {name}")
        raise AttributeError(name)
