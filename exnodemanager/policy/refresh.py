
import datetime

import exnodemanager.instruction as instruction
import exnodemanager.record as record

from policy import Policy


class Refresh(Policy):
    def __init__(self, keepfor, refreshperiod, refreshat = None, **kwargs):
        self._log = record.getLogger()
        self._keepfor = datetime.timedelta(minutes = keepfor)
        self._refreshPeriod = datetime.timedelta(minutes = refreshperiod)
        if refreshat:
            self._refreshAt = datetime.timedelta(minutes = refreshat)
        else:
            self._refreshAt = datetime.timedelta(minutes = refreshperiod / 2)
        super(Refresh, self).__init__()
        
    def _apply(self, exnode):
        self._log.info("Refreshing: {exnode}".format(exnode = exnode["raw"]["id"]))
        commands = []
        for key, adapter in exnode["allocations"].items():
            alloc = adapter.GetMetadata()
            now   = datetime.datetime.utcnow()
            timeRemaining  = alloc.end - now

            self._log.debug("  Checking allocation: Time Remaining - {tr}     Refresh At: {ra}".format(tr = timeRemaining, ra = self._refreshAt))
            if alloc.end < now or now - alloc.start > self._keepfor:
                self._log.debug("refresh.apply: RELEASE {end} - {now} [{keep}]".format(end = alloc.end, now = now, keep = self._keepfor))
                command = {}
                command["allocation"] = adapter
                command["type"]       = instruction.RELEASE
                commands.append(command)
            elif timeRemaining < self._refreshAt:
                self._log.debug("refresh.apply: REFRESH {remaining} [{refreshAt}]".format(remaining = timeRemaining, refreshAt = self._refreshAt))
                command = {}
                command["type"]       = instruction.REFRESH
                command["duration"]   = self._refreshPeriod.seconds + (self._refreshPeriod.days * 24 * 60 * 60)
                command["allocation"] = adapter
                commands.append(command)
                
        return commands


    
