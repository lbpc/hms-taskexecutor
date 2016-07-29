class Task:
	def __init__(self):
		self._id = str()
		self._res_type = str()
		self._action = str()
		self._params = dict()

	@property
	def id(self):
		return self._id

	@id.setter
	def id(self, value):
		self._id = value

	@id.deleter
	def id(self):
		del self._id

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
		self._params = value

	@params.deleter
	def params(self):
		del self._params

	def __str__(self):
		return "Task(id='{0.id}', " \
		       "res_type='{0.res_type}', " \
		       "action='{0.action}', " \
		       "params={0.params})".format(self)
