import collections

NEW = 0
PROCESSING = 1
DONE = 2
FAILED = 3


class PropertyValidationError(Exception):
    pass


class Task:
    def __init__(self, tag, origin, opid, actid, res_type, action, params):
        self._res_type = str()
        self._action = str()
        self.state = NEW
        self.tag = tag
        self.origin = origin
        self.opid = opid
        self.actid = actid
        self.res_type = res_type
        self.action = action
        self.params = params

    @property
    def origin(self):
        return getattr(self, "_origin", None)

    @origin.setter
    def origin(self, value):
        if not isinstance(value, type):
            raise PropertyValidationError("origin must be type")
        self._origin = value

    @property
    def res_type(self):
        return self._res_type

    @res_type.setter
    def res_type(self, value):
        self._res_type = value

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, value):
        self._action = value

    @property
    def params(self):
        return getattr(self, "_params", {})

    @params.setter
    def params(self, value):
        if not isinstance(value, collections.Mapping):
            raise PropertyValidationError("params must be mapping")
        self._params = value

    @property
    def state(self):
        return getattr(self, "_state", NEW)

    @state.setter
    def state(self, value):
        if value not in (NEW, PROCESSING, DONE, FAILED):
            raise PropertyValidationError("Unknown task state: {}".format(value))
        self._state = value


    def __str__(self):
        return "Task(state={0.state} " \
               "tag={0.tag}, " \
               "origin={0.origin}, " \
               "opid={0.opid}, " \
               "actid={0.actid}, " \
               "res_type={0.res_type}, " \
               "action={0.action}, " \
               "params={0.params})".format(self)
