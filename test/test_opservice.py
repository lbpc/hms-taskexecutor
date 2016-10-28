import unittest


class TestUpstartService(unittest.TestCase):
    def test_reload(self):
        # upstart_service = UpstartService()
        # self.assertEqual(expected, upstart_service.reload())
        assert True # TODO: implement your test here

    def test_restart(self):
        # upstart_service = UpstartService()
        # self.assertEqual(expected, upstart_service.restart())
        assert True # TODO: implement your test here

    def test_start(self):
        # upstart_service = UpstartService()
        # self.assertEqual(expected, upstart_service.start())
        assert True # TODO: implement your test here

    def test_stop(self):
        # upstart_service = UpstartService()
        # self.assertEqual(expected, upstart_service.stop())
        assert True # TODO: implement your test here

class TestSysVService(unittest.TestCase):
    def test_reload(self):
        # sys_v_service = SysVService()
        # self.assertEqual(expected, sys_v_service.reload())
        assert True # TODO: implement your test here

    def test_restart(self):
        # sys_v_service = SysVService()
        # self.assertEqual(expected, sys_v_service.restart())
        assert True # TODO: implement your test here

    def test_start(self):
        # sys_v_service = SysVService()
        # self.assertEqual(expected, sys_v_service.start())
        assert True # TODO: implement your test here

    def test_stop(self):
        # sys_v_service = SysVService()
        # self.assertEqual(expected, sys_v_service.stop())
        assert True # TODO: implement your test here

class TestNginx(unittest.TestCase):
    def test___init__(self):
        # nginx = Nginx()
        assert True # TODO: implement your test here

    def test_reload(self):
        # nginx = Nginx()
        # self.assertEqual(expected, nginx.reload())
        assert True # TODO: implement your test here

class TestApache(unittest.TestCase):
    def test___init__(self):
        # apache = Apache(name)
        assert True # TODO: implement your test here

    def test_reload(self):
        # apache = Apache(name)
        # self.assertEqual(expected, apache.reload())
        assert True # TODO: implement your test here

class TestUnmanagedNginx(unittest.TestCase):
    def test___init__(self):
        # unmanaged_nginx = UnmanagedNginx()
        assert True # TODO: implement your test here

    def test_reload(self):
        # unmanaged_nginx = UnmanagedNginx()
        # self.assertEqual(expected, unmanaged_nginx.reload())
        assert True # TODO: implement your test here

    def test_restart(self):
        # unmanaged_nginx = UnmanagedNginx()
        # self.assertEqual(expected, unmanaged_nginx.restart())
        assert True # TODO: implement your test here

    def test_start(self):
        # unmanaged_nginx = UnmanagedNginx()
        # self.assertEqual(expected, unmanaged_nginx.start())
        assert True # TODO: implement your test here

    def test_stop(self):
        # unmanaged_nginx = UnmanagedNginx()
        # self.assertEqual(expected, unmanaged_nginx.stop())
        assert True # TODO: implement your test here

class TestUnmanagedApache(unittest.TestCase):
    def test___init__(self):
        # unmanaged_apache = UnmanagedApache(name)
        assert True # TODO: implement your test here

    def test_reload(self):
        # unmanaged_apache = UnmanagedApache(name)
        # self.assertEqual(expected, unmanaged_apache.reload())
        assert True # TODO: implement your test here

    def test_restart(self):
        # unmanaged_apache = UnmanagedApache(name)
        # self.assertEqual(expected, unmanaged_apache.restart())
        assert True # TODO: implement your test here

    def test_start(self):
        # unmanaged_apache = UnmanagedApache(name)
        # self.assertEqual(expected, unmanaged_apache.start())
        assert True # TODO: implement your test here

    def test_stop(self):
        # unmanaged_apache = UnmanagedApache(name)
        # self.assertEqual(expected, unmanaged_apache.stop())
        assert True # TODO: implement your test here

if __name__ == '__main__':
    unittest.main()
