
####################
# Logging Settings #
####################

WORKSPACE  = "."


####################
# Control Settings #
####################

ITERATION_TIME = 20 # Time between checks in seconds


########################
# Concurrency Settings #
########################

THREADS = 20




#####################
#  Policy Settings  #
#####################

policies = [
    {
        "class": "exnodemanager.policy.refresh.Refresh",
        "args":  { "keepfor": 24 * 32 * 60, "refreshperiod": 60 * 20 },
        "filters": []
    },
#    {
#        "class": "exnodemanager.policy.maintaincopies.MaintainCopies",
#        "args": { "copies": 3 },
#        "filters": []
#    },
#    {
#        "class": "exnodemanager.policy.oneat.OneAt",
#        "args": { "host": "nadir.ersc.wisc.edu", "port": 6714 },
#        "filters": [
#            {
#                "class": "exnodemanager.filter.usgsrowpath.RowPath",
#                "args": { "startPath": 23, "endPath": 26, "startRow": 26, "endRow": 31 }
#            },
#            {
#                "class": "exnodemanager.filter.usgsfiletype.FileType",
#                "args": { "types": [".zip", ".tar.gz"] }
#            }
#        ]   
#    }
]
