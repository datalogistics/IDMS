
import logging
import settings
import os

def getLogger(namespace = "exnodemanager", level = logging.WARNING):
    logger = logging.getLogger(namespace)

    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False
    
    logfile = "{workspace}/log".format(workspace = settings.WORKSPACE)
    if not os.path.isdir(logfile):
        os.makedirs(logfile)

    formatter = logging.Formatter("%(asctime)s %(levelname)7s| %(message)s", "%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler("{directory}/manager.log".format(directory = logfile), mode = 'a+')
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    stdout = logging.StreamHandler()
    stdout.setLevel(logging.DEBUG)
    stdout.setFormatter(formatter)
    logger.addHandler(stdout)
    
    return logger
