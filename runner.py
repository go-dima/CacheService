import logging
import threading
import time

from chache_service import CacheService


def write(cache_service: CacheService):
    index = 0
    while True:
        logging.info(f"Writer: key{index}, value{index}")
        cache_service.set(f"key{index}", f"value{index}", expiry=20)
        index += 1
        time.sleep(7)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cache = CacheService()

    t1 = threading.Thread(target=cache.run)
    t1.start()

    t2 = threading.Thread(target=write, args=(cache,))
    t2.start()

    t1.join()
    t2.join()
