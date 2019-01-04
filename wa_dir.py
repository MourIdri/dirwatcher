import threading
import inotify.adapters
import multiprocessing  
import hashlib  
import os  
import threading  
import time
import logging
from logging.handlers import RotatingFileHandler
import json
import inotify.adapters
import sys
import datetime 

from multiprocessing import Process
from os import environ
from os.path import join, dirname

_DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_LOGGER = logging.getLogger(__name__)


def runInParallel(*fns):
  proc = []
  for fn in fns:
    p = Process(target=fn)
    p.start()
    proc.append(p)
  for p in proc:
    p.join()


def _configure_logging():
    _LOGGER.setLevel(logging.INFO)
    fileLogName='LOG_directory_operations.log'
    ch = logging.StreamHandler()
    formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
    fileHandler = logging.handlers.RotatingFileHandler(fileLogName, maxBytes=(1048576*5), backupCount=7)
    fileHandler.setFormatter(formatter)
    ch.setFormatter(formatter)

    _LOGGER.addHandler(ch)
    _LOGGER.addHandler(fileHandler)


def PopUpMessage (event):
    filepartname = ".filepart"
    filelaststate_writting="IN_CLOSE_WRITE"
    filelaststate_deleting="IN_DELETE"
    filelaststate_in_attrib="IN_ATTRIB"
    if event is not None:
        (header, type_names, watch_path, filename) = event
        _LOGGER.info("WD=(%d) MASK=(%d) COOKIE=(%d) LEN=(%d) MASK->NAMES=%s "
            "WATCH-PATH=[%s] FILENAME=[%s]",
            header.wd, header.mask, header.cookie, header.len, type_names,
            watch_path.decode('utf-8'), filename.decode('utf-8'))
        time.sleep(0.5)
        if filepartname in filename :
            print ( "\n")
            _LOGGER.info('FUNCTION // PopUpMessage // file is still uploading localy it still not complete, the analysis will be done once the file will be completely uploaded')
            print ( "\n")
        if filelaststate_deleting in type_names and filepartname not in filename:
            print ( "\n")
            _LOGGER.info('FUNCTION // PopUpMessage // file is being deleted from the local repository')
            print ( "\n")
        if filelaststate_deleting in type_names and filepartname in filename:
            print ( "\n")
            _LOGGER.info('FUNCTION // PopUpMessage // part file and its attributes are being deleted from the local repository')
            print ( "\n")
        if filelaststate_in_attrib in type_names  :
            if not filename:
                pass
            else :
                print ( "\n")
                _LOGGER.info('FUNCTION // PopUpMessage // file completely uploaded localy, attributes added to files ')
                print ( "\n")
                global filepushedforwatsonanalysis
                filepushedforwatsonanalysis=str(filename)
                tempessage =str (("FUNCTION // PopUpMessage // the file that is going to be sent to processed is : %s") % (filepushedforwatsonanalysis ))
                _LOGGER.info(tempessage)
                print ( "\n")
                #maincallFunctionsPage(filepushedforwatsonanalysis)


def My_main(path):
    i = inotify.adapters.Inotify()
    DirWatcher=i.add_watch(path)
    try:
        while True: 
            for event in i.event_gen():
                m = multiprocessing.Process(target=PopUpMessage, args=(event,))
                m.start()            
    finally:
        i.remove_watch(b'/PARA')



if __name__ == '__main__':
    _configure_logging()
    path = "/mnt/c/code_source/GENERAL/HPC/PARA/PATHTEST/"
    N = multiprocessing.Process(target=My_main, args=(path,))
    N.start()