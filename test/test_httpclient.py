import unittest

class TestHttpClient(unittest.TestCase):
    def test___enter__(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.__enter__())
        assert False # TODO: implement your test here

    def test___exit__(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.__exit__(exc_type, exc_val, exc_tb))
        assert False # TODO: implement your test here

    def test___init__(self):
        # http_client = HttpClient(address, port)
        assert False # TODO: implement your test here

    def test_decode_response(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.decode_response())
        assert False # TODO: implement your test here

    def test_delete(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.delete(uri_path, headers))
        assert False # TODO: implement your test here

    def test_get(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.get(uri_path, headers))
        assert False # TODO: implement your test here

    def test_post(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.post(body, uri_path, headers))
        assert False # TODO: implement your test here

    def test_process_response(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.process_response(method, response))
        assert False # TODO: implement your test here

    def test_put(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.put(body, uri_path, headers))
        assert False # TODO: implement your test here

    def test_uri_path(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.uri_path())
        assert False # TODO: implement your test here

    def test_uri_path_case_2(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.uri_path(value))
        assert False # TODO: implement your test here

    def test_uri_path_case_3(self):
        # http_client = HttpClient(address, port)
        # self.assertEqual(expected, http_client.uri_path())
        assert False # TODO: implement your test here

class TestApiClient(unittest.TestCase):
    def test___getattr__(self):
        # api_client = ApiClient(address, port, service)
        # self.assertEqual(expected, api_client.__getattr__(name))
        assert False # TODO: implement your test here

    def test___init__(self):
        # api_client = ApiClient(address, port, service)
        assert False # TODO: implement your test here

    def test_process_response(self):
        # api_client = ApiClient(address, port, service)
        # self.assertEqual(expected, api_client.process_response(method, response))
        assert False # TODO: implement your test here

class TestConfigServerClient(unittest.TestCase):
    def test___init__(self):
        # config_server_client = ConfigServerClient(address, port)
        assert False # TODO: implement your test here

    def test_extra_attrs(self):
        # config_server_client = ConfigServerClient(address, port)
        # self.assertEqual(expected, config_server_client.extra_attrs())
        assert False # TODO: implement your test here

    def test_extra_attrs_case_2(self):
        # config_server_client = ConfigServerClient(address, port)
        # self.assertEqual(expected, config_server_client.extra_attrs(lst))
        assert False # TODO: implement your test here

    def test_extra_attrs_case_3(self):
        # config_server_client = ConfigServerClient(address, port)
        # self.assertEqual(expected, config_server_client.extra_attrs())
        assert False # TODO: implement your test here

    def test_get_property_source(self):
        # config_server_client = ConfigServerClient(address, port)
        # self.assertEqual(expected, config_server_client.get_property_source(name, profile, source_name))
        assert False # TODO: implement your test here

    def test_get_property_sources_list(self):
        # config_server_client = ConfigServerClient(address, port)
        # self.assertEqual(expected, config_server_client.get_property_sources_list(name, profile))
        assert False # TODO: implement your test here

    def test_process_response(self):
        # config_server_client = ConfigServerClient(address, port)
        # self.assertEqual(expected, config_server_client.process_response(method, response))
        assert False # TODO: implement your test here

class TestEurekaClient(unittest.TestCase):
    def test___init__(self):
        # eureka_client = EurekaClient(address, port)
        assert False # TODO: implement your test here

    def test_get_instances_list(self):
        # eureka_client = EurekaClient(address, port)
        # self.assertEqual(expected, eureka_client.get_instances_list(application_name))
        assert False # TODO: implement your test here

    def test_get_random_instance(self):
        # eureka_client = EurekaClient(address, port)
        # self.assertEqual(expected, eureka_client.get_random_instance(application_name))
        assert False # TODO: implement your test here

    def test_process_response(self):
        # eureka_client = EurekaClient(address, port)
        # self.assertEqual(expected, eureka_client.process_response(method, response))
        assert False # TODO: implement your test here

class TestApiObjectTranslator(unittest.TestCase):
    def test___init__(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        assert False # TODO: implement your test here

    def test_as_dict(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.as_dict())
        assert False # TODO: implement your test here

    def test_as_object(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.as_object(extra_attrs, overwrite, expand_dot_separated))
        assert False # TODO: implement your test here

    def test_cast_numeric_recursively(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.cast_numeric_recursively())
        assert False # TODO: implement your test here

    def test_dict_merge(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.dict_merge(*args, overwrite))
        assert False # TODO: implement your test here

    def test_namedtuple_from_mapping(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.namedtuple_from_mapping(name))
        assert False # TODO: implement your test here

    def test_object_hook(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.object_hook(extra, overwrite, expand))
        assert False # TODO: implement your test here

    def test_to_namedtuple(self):
        # api_object_translator = ApiObjectTranslator(json_string)
        # self.assertEqual(expected, api_object_translator.to_namedtuple())
        assert False # TODO: implement your test here

if __name__ == '__main__':
    unittest.main()
