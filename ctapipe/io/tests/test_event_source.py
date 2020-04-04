import pytest
from ctapipe.utils import get_dataset_path
from ctapipe.io.eventsource import EventSource


def test_construct():
    with pytest.raises(TypeError):
        EventSource()


class DummyReader(EventSource):
    """
    Simple working EventSource
    """

    def _generator(self):
        return range(len(self.input_url))

    @staticmethod
    def is_compatible(file_path):
        return False

    @property
    def subarray(self):
        return None


def test_can_be_implemented():
    dataset = get_dataset_path("gamma_test_large.simtel.gz")
    test_reader = DummyReader(input_url=dataset)
    assert test_reader is not None


def test_is_iterable():
    dataset = get_dataset_path("gamma_test_large.simtel.gz")
    test_reader = DummyReader(input_url=dataset)
    for _ in test_reader:
        pass


def test_from_url_config():
    from ctapipe.core import Component
    from traitlets.config import Config

    path = get_dataset_path('gamma_test_large.simtel.gz')

    config = Config()
    config.input_url = path

    e = EventSource.from_url(config=config)
    assert e.input_url == path

    class TestComponent(Component):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.eventsource = EventSource.from_url(parent=self)

    parent_config = Config()
    parent_config.EventSource.input_url = path
    t = TestComponent(config=parent_config)
    assert t.eventsource.input_url == path
