from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.dbclient import MySQLClient
from taskexecutor.httpclient import ApiClient
from taskexecutor.utils import repquota, set_thread_name


class FactsGatherer:
    def get_quota(self, res_type):
        if res_type == "unix-account" and CONFIG.hostname == "baton":
            return repquota(freebsd=True)
        elif res_type == "unix-account":
            return repquota()
        elif res_type == "database":
            with MySQLClient(database="information_schema",
                             **CONFIG.mysql) as c:
                c.execute("SELECT table_schema, SUM(data_length+index_length) "
                          "FROM TABLES GROUP BY table_schema")

                return dict(c.fetchall())


class FactsSender:
    def __init__(self, res_type, fact_type):
        self._res_type = str()
        self._fact_type = str()
        self._facts = dict()
        self._resources = list()
        self.res_type = res_type
        self.fact_type = fact_type

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
    def fact_type(self):
        return self._fact_type

    @fact_type.setter
    def fact_type(self, value):
        self._fact_type = value

    @fact_type.deleter
    def fact_type(self):
        del self._fact_type

    @property
    def facts(self):
        return self._facts

    @facts.setter
    def facts(self, value):
        self._facts = value

    @facts.deleter
    def facts(self):
        del self._facts

    @property
    def resources(self):
        return self._resources

    @resources.setter
    def resources(self, value):
        self._resources = value

    @resources.deleter
    def resources(self):
        del self._resources

    def get_facts(self):
        if self.fact_type == "quota":
            self.facts = FactsGatherer().get_quota(self.res_type)
        return self.facts

    def get_resources(self):
        with ApiClient(**CONFIG.apigw) as api:
            Resources = getattr(api, self.res_type)
            self.resources = Resources(
                    query={"serverId": CONFIG.localserver.id}).get()
        return self.resources

    def send_facts(self):
        LOGGER.debug("Gathered facts: {}".format(self.facts))
        for resource in self.resources:
            if self.res_type == "unix-account":
                data = self.facts[resource.uid]["block_limit"]["used"]
            elif self.res_type == "database":
                data = self.facts[resource.name]
            LOGGER.info("Reporting fact: {0} {1} {2} "
                        "is {3}".format(self.res_type,
                                        resource.name,
                                        self.fact_type,
                                        data))
            with ApiClient(**CONFIG.rc_user) as api:
                ApiResource = getattr(api, self.res_type)
                ApiResourceFact = getattr(ApiResource(resource.id),
                                          self.fact_type)
                ApiResourceFact(res_id=str(data)).post(None)

    def update(self):
        set_thread_name("FactsSender")
        if self.get_resources() and self.get_facts():
            self.send_facts()
