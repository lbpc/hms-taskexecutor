class PropertyValidationError(Exception):
    pass


class Task:
    def __init__(self, opid, actid, res_type, action, params):
        self._opid = str()
        self._actid = str()
        self._res_type = str()
        self._action = str()
        self._params = dict()
        self.opid = opid
        self.actid = actid
        self.res_type = res_type
        self.action = action
        self.params = params

    @property
    def opid(self):
        return self._opid

    @opid.setter
    def opid(self, value):
        self._opid = value

    @opid.deleter
    def opid(self):
        del self._opid

    @property
    def actid(self):
        return self._actid

    @actid.setter
    def actid(self, value):
        self._actid = value

    @actid.deleter
    def actid(self):
        del self._actid

    @property
    def res_type(self):
        return self._res_type

    @res_type.setter
    def res_type(self, value):
        self._res_type = value

    @res_type.deleter
    def res_type(self):
        del self._res_type

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, value):
        self._action = value

    @action.deleter
    def action(self):
        del self._action

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, value):
        if type(value) != dict:
            raise PropertyValidationError("params must be dict")
        self._params = value

    @params.deleter
    def params(self):
        del self._params

    def __str__(self):
        return "Task(opid='{0.opid}', " \
               "actid='{0.actid}', " \
               "res_type='{0.res_type}', " \
               "action='{0.action}', " \
               "params={0.params})".format(self)
