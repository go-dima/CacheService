import time
import unittest

from chache_service import CacheService


class CacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = CacheService()

    def test__add__value_exists(self):
        # Arrange + Act
        self.cache.set("key1", "data_w1")

        # Assert
        self.assertEqual("data_w1", self.cache.get("key1"))

    def test__simple_add_and_remove__returns_value(self):
        # Arrange
        self.cache.set("key1", "data_w1")

        # Act
        self.cache.delete("key1")

        # Assert
        self.assertEqual(None, self.cache.get("key1"))

    def test__add_and_wait__returns_none(self):
        # Arrange
        expiry_interval = 5
        self.cache.set("key1", "data_w1", expiry_interval)

        # Act
        time.sleep(expiry_interval)

        # Assert
        self.assertEqual(None, self.cache.get("key1"))
