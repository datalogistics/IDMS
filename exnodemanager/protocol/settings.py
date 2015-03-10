

DEFAULT_PASSWORD = "ibp"
DEFAULT_TIMEOUT  = 30
DEFAULT_MAXSIZE  = 1024 * 1024 * 10

PROTOCOL_VERSION = 0
IBP_ST_INQ       = 1
IBP_STATUS       = 4
IBP_ALLOCATE     = 1
IBP_STORE        = 2
IBP_SEND         = 5
IBP_MANAGE       = 9
IBP_CHANGE       = 43
IBP_PROBE        = 40

####################
#  Reliability     #
####################
IBP_SOFT         = 1
IBP_HARD         = 2

####################
#  Storage Type    #
####################
IBP_BYTEARRAY = 1
IBP_BUFFER    = 2
IBP_FIFO      = 3
IBP_CIRQ      = 4


DEFAULT_DURATION = 60 * 60 * 24 # 32 hours
