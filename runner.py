import logging
import threading
import time

from chache_service import CacheService


def write(cache_service: CacheService, uid):
    index = 0
    while True:
        logging.info(f"Writer{uid}: key_{uid*'1'}_{index}, value{uid}")
        cache_service.set(f"key{uid}", f"value{uid}", expiry=3)

        index += 1
        time.sleep(7)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cache = CacheService()

    t1 = threading.Thread(target=cache.run)
    t1.start()

    for i in range(1, 3):
        t = threading.Thread(target=write, args=(cache, i))
        t.start()
