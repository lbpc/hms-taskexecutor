import collections

NEW = 0
PROCESSING = 1
DONE = 2
FAILED = 3


class PropertyValidationError(Exception):
    pass


class Task:
    def __init__(self, tag, origin, opid, actid, res_type, action, params):
        self._state = str()
        self._tag = None
        self._origin = None
        self._opid = str()
        self._actid = str()
        self._res_type = str()
        self._action = str()
        self._params = dict()
        self.state = NEW
        self.tag = tag
        self._origin = origin
        self.opid = opid
        self.actid = actid
        self.res_type = res_type
        self.action = action
        self.params = params

    @property
    def tag(self):
        return self._tag

    @tag.setter
    def tag(self, value):
        self._tag = value

    @tag.deleter
    def tag(self):
        del self._tag

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, value):
        if not isinstance(value, type):
            raise PropertyValidationError("origin must be type")
        self._origin = value

    @origin.deleter
    def origin(self):
        del self._origin

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
        if not isinstance(value, collections.Mapping):
            raise PropertyValidationError("params must be mapping")
        self._params = value

    @params.deleter
    def params(self):
        del self._params

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in (NEW, PROCESSING, DONE, FAILED):
            raise PropertyValidationError("Unknown task state: {}".format(value))
        self._state = value

    @state.deleter
    def state(self):
        del self._state

    def __str__(self):
        return "Task(state={0.state} " \
               "tag={0.tag}, " \
               "origin={0.origin}, " \
               "opid={0.opid}, " \
               "actid={0.actid}, " \
               "res_type={0.res_type}, " \
               "action={0.action}, " \
               "params={0.params})".format(self)