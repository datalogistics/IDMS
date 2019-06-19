from lace import logging
from threading import Thread
from time import sleep

from idms.settings import ENGINE_LOOP_DELAY

def run(db):
    log = logging.getLogger("idms.engine")
    def _loop():
        while True:
            sleep(ENGINE_LOOP_DELAY)
            try:
                [p.apply(db) for p in db.get_active_policies()]
                if list(db.get_active_policies()):
                    stats = [p.status for p in db.get_active_policies()]
                    _print_status([p.status for p in db.get_active_policies()])
                    log.debug(stats)
            except Exception as exp:
                log.warn("Failure during policy application - {}".format(exp))

    runner = Thread(target=_loop, name="idms_engine", daemon=True)
    runner.start()

def _print_status(status):
    # print top
    print("\u250c", end='')
    for _ in range(len(status) - 1):
        print("\u2500\u2500\u2500", end='')
        print("\u252c", end='')
    print("\u2500\u2500\u2500", end='')
    print("\u2510")
    
    # print ids
    print("\u2502", end='')
    for i in range(len(status)):
        print("", i, "\u2502", end='')
    print()
    
    # print headerbottom
    print("\u251c", end='')
    for _ in range(len(status) - 1):
        print("\u2500\u2500\u2500", end='')
        print("\u253c", end='')
    print("\u2500\u2500\u2500", end='')
    print("\u2524")
    
    # print statuses
    colors = ["\033[90m", "\033[92m", "\033[91m"]
    print("\u2502", end='')
    [print("", "{}\u2b24\033[0m".format(colors[s.value]), "\u2502", end='') for s in status]
    print()
    
    # print bottom
    print("\u2514", end='')
    for _ in range(len(status) - 1):
        print("\u2500\u2500\u2500", end='')
        print("\u2534", end='')
    print("\u2500\u2500\u2500", end='')
    print("\u2518")
