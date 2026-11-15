"""`install_companion` refuses a Spark Connect session by itself, with a Fix, before any JVM access."""

from databricks.labs.gbx.manager import jvm

from .connect_stub import ConnectStub

FQN = "com.databricks.labs.gbx.kernels.SomeCompanion"


def test_connect_session_is_refused_with_a_fix_and_no_jvm_access():
    spark = ConnectStub()
    why = jvm.install_companion(spark, FQN, "gbx__l1_x")
    assert why is not None and why.startswith("gbx__l1_x: ")
    assert "Spark Connect" in why and "Fix:" in why
    assert spark.touched == []  # neither _jvm, _jsparkSession nor conf.set was reached


def test_private_name_form_refuses_the_same_way():
    spark = ConnectStub()
    why = jvm.install_private_l1(spark, FQN, "gbx__l1_y")
    assert why is not None and "Fix:" in why
    assert spark.touched == []
