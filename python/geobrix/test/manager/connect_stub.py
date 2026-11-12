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


class _DataSource:
    def __init__(self, owner):
        self.owner = owner

    def register(self, cls):
        self.owner.sources.append(cls.name())


class _ReadOnlyConf:
    """`spark.conf` as a Connect session lets the library use it: read (`get`), never set."""

    def __init__(self, owner):
        self.owner = owner

    def get(self, key, default=None):
        return default

    def __getattr__(self, name):
        self.owner.__dict__.setdefault("touched", []).append(f"conf.{name}")
        raise AssertionError(f"forbidden on Connect: conf.{name}")


class ConnectStub:
    def __init__(self):
        self.touched, self.registered, self.sql_text = [], [], []
        self.artifacts, self.java_registered, self.sandbox_classes = [], [], set()
        self.sources = []
        self.udf = _Udf(self)
        self.udtf = _Udtf(self)
        self.dataSource = _DataSource(self)
        self.conf = _ReadOnlyConf(self)

    def sql(self, text):
        self.sql_text.append(text)
        # the probes the manager runs once its Java handlers are registered
        from databricks.labs.gbx import __version__

        class _Row:
            def first(_self):
                return {"i": f"{__version__}|abc", "g": "ok:3.13.3"}

        return _Row()

    def addArtifact(self, *path, **kw):
        self.artifacts.extend(path)

    def __getattr__(self, name):
        if name in ("_jvm", "_jsc", "sparkContext", "_jsparkSession"):
            self.__dict__.setdefault("touched", []).append(name)
            raise AssertionError(f"forbidden on Connect: {name}")
        raise AttributeError(name)


class _Ext:
    """A ``google.protobuf.Any`` look-alike: the two fields a user-context extension carries."""

    def __init__(self, type_url: str, value: bytes):
        self.type_url, self.value = type_url, value


class FlowClient:
    """The user-context extensions of a Spark Connect client (``SparkConnectClient``), as Lakeflow uses them.

    A pipeline runs each dataset function under ``add_pipeline_analysis_context``: one thread-local
    ``PipelineAnalysisContext`` extension (the graph id and the flow name) that every request of that thread carries.
    ``flow(name)`` is that context manager; ``current()`` is the flow the calling thread's requests carry now.
    """

    PIPELINE = "type.googleapis.com/spark.connect.PipelineAnalysisContext"

    def __init__(self):
        import threading
        import uuid

        self._uuid = uuid
        self.thread_local = threading.local()
        self.global_user_context_extensions = []

    def add_threadlocal_user_context_extension(self, extension) -> str:
        if not hasattr(self.thread_local, "user_context_extensions"):
            self.thread_local.user_context_extensions = []
        ext_id = "threadlocal_" + str(self._uuid.uuid4())
        self.thread_local.user_context_extensions.append((ext_id, extension))
        return ext_id

    def add_global_user_context_extension(self, extension) -> str:
        ext_id = "global_" + str(self._uuid.uuid4())
        self.global_user_context_extensions.append((ext_id, extension))
        return ext_id

    def remove_user_context_extension(self, ext_id: str) -> None:
        if ext_id.startswith("threadlocal_"):
            self.thread_local.user_context_extensions = [
                e
                for e in getattr(self.thread_local, "user_context_extensions", [])
                if e[0] != ext_id
            ]
        else:
            self.global_user_context_extensions = [
                e for e in self.global_user_context_extensions if e[0] != ext_id
            ]

    def flow(self, name: str, graph: str = "g1"):
        import contextlib

        @contextlib.contextmanager
        def ctx():
            ext_id = self.add_threadlocal_user_context_extension(
                _Ext(self.PIPELINE, f"{graph}/{name}".encode())
            )
            try:
                yield
            finally:
                self.remove_user_context_extension(ext_id)

        return ctx()

    def current(self):
        """The flow the calling thread's requests carry (None outside a flow)."""
        for _, e in getattr(self.thread_local, "user_context_extensions", []):
            if e.type_url == self.PIPELINE:
                return e.value.decode().split("/", 1)[1]
        return None


class _FlowRecording:
    """``udf`` / ``udtf`` / ``dataSource`` of a ``FlowStub``: each registration records the flow its request carries."""

    def __init__(self, owner, inner):
        self.owner, self.inner = owner, inner

    def register(self, *a, **k):
        self.owner.flows_seen.append(self.owner.client.current())
        return self.inner.register(*a, **k)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class FlowStub(ConnectStub):
    """A Connect look-alike with a client that carries Lakeflow's per-flow analysis context; every request it
    receives (``sql`` and each registration) records the flow it carried in ``flows_seen``.
    """

    def __init__(self):
        super().__init__()
        self.client = FlowClient()
        self.flows_seen = []
        self.udf = _FlowRecording(self, self.udf)
        self.udtf = _FlowRecording(self, self.udtf)
        self.dataSource = _FlowRecording(self, self.dataSource)

    def sql(self, text):
        self.flows_seen.append(self.client.current())
        return super().sql(text)

    def round_trips(self) -> int:
        return len(self.flows_seen)
