import logging
from collections import namedtuple
from typing import Dict

import schedule
from datetime import datetime, timedelta

Data = namedtuple('Data', ['content', 'expiry'])


class CacheService:
    def __init__(self, maintenance_frequency=5):
        self.db: Dict[str, Data] = dict()
        schedule.every(maintenance_frequency).seconds.do(self.maintain)

    def set(self, key, value, expiry: int = 30):
        expiry_time = datetime.now() + timedelta(seconds=expiry)
        self.db[key] = Data(content=value, expiry=expiry_time)

    def get(self, key):
        if key in self.db:
            value = self.db.get(key)
            if value.expiry > datetime.now():
                return value.content
            else:  # Minor improvement, if key is expired but wasn't cleared yet, we'll clear it here
                self.delete(key)
        return None

    def delete(self, key):
        self.db.pop(key, None)

    def maintain(self):
        maintenance_time = datetime.now()
        current_keys = list(self.db.keys())  # Copy keys to avoid changes during iteration
        logging.info(f"Maintenance: cache has {len(current_keys)} keys")
        for key in current_keys:
            value = self.db.get(key)
            if value and value.expiry < maintenance_time:
                logging.info(f"Maintenance: {key} has expired. {value.expiry} ==== {maintenance_time}")
                self.delete(key)

    def run(self):
        while True:
            schedule.run_pending()
