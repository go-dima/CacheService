import logging
import threading
from collections import namedtuple
from typing import Dict, List

import schedule
from datetime import datetime, timedelta

Data = namedtuple('Data', ['content', 'expiry'])


class CacheService:
    def __init__(self, maintenance_frequency=5):
        self.db: List[Dict[str, Data]] = [dict(), dict()]
        self.reading_idx = 0
        self.write_lock = threading.Lock()
        schedule.every(maintenance_frequency).seconds.do(self.maintain)

    def set(self, key, value, expiry: int = 30):
        expiry_time = datetime.now() + timedelta(seconds=expiry)
        try:
            self.write_lock.acquire()
            self.write_db[key] = Data(content=value, expiry=expiry_time)
            self.flip_db()
            self.write_db[key] = Data(content=value, expiry=expiry_time)
        finally:
            self.write_lock.release()

    def get(self, key):
        if key in self.read_db:
            value = self.read_db.get(key)
            if value.expiry > datetime.now():
                return value.content
            else:  # Minor improvement, if key is expired but wasn't cleared yet, we'll clear it here
                self.delete(key)
        return None

    def delete(self, key):
        try:
            self.write_lock.acquire()
            self._safe_delete(key)
        finally:
            self.write_lock.release()

    def _safe_delete(self, key):
        self.write_db.pop(key)
        self.flip_db()
        self.write_db.pop(key)

    def maintain(self):
        try:
            self.write_lock.acquire()
            maintenance_time = datetime.now()
            current_keys = list(self.write_db.keys())  # Copy keys to avoid changes during iteration
            logging.info(f"Maintenance: cache has {len(current_keys)} keys")
            for key in current_keys:
                value = self.write_db.get(key)
                if value and value.expiry < maintenance_time:
                    logging.info(f"Maintenance: {key} has expired. {value.expiry} ==== {maintenance_time}")
                    self._safe_delete(key)
        finally:
            self.write_lock.release()

    def run(self):
        while True:
            schedule.run_pending()

    @property
    def read_db(self):
        return self.db[self.reading_idx]

    @property
    def write_db(self):
        return self.db[(self.reading_idx + 1) % 2]

    def flip_db(self):
        self.reading_idx = (self.reading_idx + 1) % 2
