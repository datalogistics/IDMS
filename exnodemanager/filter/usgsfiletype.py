

class FileType(object):
    def __init__(self, types):
        self._types = types

    def Apply(self, exnode):
        exnode = exnode["raw"]
        try:
            filetype = exnode["name"].split('.', 1)[1]
        except Exception as exp:
            return False
            
        for _type in self._types:
            if filetype == _type:
                return True

        return False
