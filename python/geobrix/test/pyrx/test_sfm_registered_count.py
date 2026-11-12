"""The registered-image count includes the initial pair.

pycolmap fires ``next_image_callback`` once per image registered AFTER the initial pair, and
``initial_image_pair_callback`` once for the pair itself. A count driven by the next-image
callback alone is short by two per model (it printed 51/53 for a model holding 53 images).
"""

from databricks.labs.gbx.rasterx.sfm import _RegistrationCounter


def test_initial_pair_is_counted():
    c = _RegistrationCounter()
    c.on_init()
    assert c.value == 2


def test_count_matches_model_size_after_all_images_register():
    # 53-image model: the init pair, then 51 next-image callbacks.
    c = _RegistrationCounter()
    c.on_init()
    for _ in range(51):
        c.on_next()
    assert c.value == 53


def test_each_model_adds_its_own_initial_pair():
    # A second sub-model starts with its own initial pair.
    c = _RegistrationCounter()
    c.on_init()
    c.on_next()
    c.on_init()
    assert c.value == 5


def test_no_callbacks_means_zero():
    assert _RegistrationCounter().value == 0
