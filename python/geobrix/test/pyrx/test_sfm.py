from databricks.labs.gbx.pyrx.sfm import image_ids_to_pair_id

_MAX = 2147483647

def test_pair_id_basic():
    assert image_ids_to_pair_id(1, 2) == _MAX * 1 + 2

def test_pair_id_swap_invariant():
    # id1 > id2 must produce the SAME key as the sorted order
    assert image_ids_to_pair_id(5, 3) == image_ids_to_pair_id(3, 5) == _MAX * 3 + 5

def test_pair_id_equal_ids():
    assert image_ids_to_pair_id(7, 7) == _MAX * 7 + 7
