import psutil
from textwrap import dedent
from unittest.mock import patch, Mock

from pyfakefs.fake_filesystem_unittest import TestCase

from .mock_config import CONFIG

CONFIG.opservice.config_templates_cache = '/nowhere/cache'

import taskexecutor.builtinservice as bs

class TestLinuxUserManager(TestCase):
    def setUp(self):
        CONFIG.builtinservice.linux_user_manager = Mock()
        CONFIG.builtinservice.sysconf_dir = '/nowhere/etc'
        CONFIG.conffile.tmp_dir = '/nowhere/conf'
        CONFIG.builtinservice.linux_user_manager.min_uid = 5
        self.setUpPyfakefs()

    def test_id_from_config(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            user0:x:0:0:Test User,,,:/home/user0:/bin/bash
            user1:x:1:1:Test User,,,:/home/user1:/bin/bash
            user2:x:3:3:Test User,,,:/home/user2:/bin/bash
            user3:x:5:5:Test User,,,:/home/user3:/bin/bash
            user4:x:999:999:Test User,,,:/home/user4:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            group0:x:0:
            group1:x:1:
            group2:x:2:
            group6626:x:6626:
            gid_forgotten:x::
            trash
        """).lstrip())
        mgr = bs.LinuxUserManager()
        self.assertEqual(mgr._id_from_config(mgr._etc_passwd, 'user1'), 1)
        self.assertEqual(mgr._id_from_config(mgr._etc_passwd, 'user5'), 2)
        self.assertEqual(mgr._id_from_config(mgr._etc_group, 'group6626'), 6626)
        self.assertEqual(mgr._id_from_config(mgr._etc_group, 'group3'), 3)
        self.assertEqual(mgr._id_from_config(mgr._etc_group, 'gid_forgotten'), 3)
        self.assertEqual(mgr._id_from_config(mgr._etc_group, 'trash'), 3)
        CONFIG.builtinservice.linux_user_manager.min_uid = 2
        self.assertRaises(bs.IdConflict, mgr._id_from_config, mgr._etc_group, 'group3')

    def test_default_shell(self):
        CONFIG.builtinservice.linux_user_manager.default_shell = '/bin/zsh'
        self.assertEqual(bs.LinuxUserManager().default_shell, '/bin/zsh')
        CONFIG.builtinservice.linux_user_manager = None
        self.assertEqual(bs.LinuxUserManager().default_shell, '/bin/bash')

    def test_disabled_shell(self):
        CONFIG.builtinservice.linux_user_manager.disabled_shell = '/usr/sbin/nologin'
        self.assertEqual(bs.LinuxUserManager().disabled_shell, '/usr/sbin/nologin')
        CONFIG.builtinservice.linux_user_manager = None
        self.assertEqual(bs.LinuxUserManager().disabled_shell, '/bin/false')

    def test_get_user(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            user0:x:1000:1000:Test User,,,:/home/user0:/bin/bash
            user1:x:1001:1001:Test User,,,:/home/user1:/bin/false
            user1:x:9999:9999:Test User,,,:/home/user1:/bin/false
            user2:x:1002:1002:Test User,,,:/home/user2:/bin/bash
            user3:x:1003:1003:Test User,,,:/home/user3:/bin/bash
            user4:x:1004:1004:Test User,,,:/home/user4:/bin/bash
            user5:m:e:s:s:e:d:u:p
            user6:x:onethousandandsix:1006:Test User,,,:/home/user6:/bin/bash
            user7:x:1007:1007.5:Test User,,,:/home/user7:/bin/bash
            user8:x:1008:1008:Test User,,,:not/an/absolute/path:/bin/bash
            user9:x:1009:1009:Test User,,,:/home/user9:not/an/absolute/path
            user10::1010:1010:Test User,,,:/home/user10:/bin/bash
            user11:!:1011:1011:Test User,,,:/home/user11:/bin/bash
            user12:*:1012:1012:Test User,,,:/home/user12:/bin/bash
            user13:x:1013:1013::/home/user13:/bin/bash
            user14:x:1014:1014:Test User,,,:/home/user14:/bin/bash
            user15:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:1015:1015:Test User,,,:/home/user15:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            user0:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:16956:0:99999:7:::
            user1:!:18354:0:99999:7:::
            user3:!:18354:0:99999:7:::
            user4:w:t:f:
            user6:!:16956:0:99999:7:::
            user7:!:16956:0:99999:7:::
            user8:!:16956:0:99999:7:::
            user9:!:16956:0:99999:7:::
            user13:!:16956:0:99999:7:::
            user14:!$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:16956:0:99999:7:::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        u0 = mgr.get_user('user0')
        self.assertIsInstance(u0, bs.User)
        self.assertEqual(u0.name, 'user0')
        self.assertEqual(u0.uid, 1000)
        self.assertEqual(u0.gid, 1000)
        self.assertEqual(u0.password_hash, '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0')
        self.assertEqual(u0.gecos, 'Test User,,,')
        self.assertEqual(u0.home, '/home/user0')
        self.assertEqual(u0.shell, '/bin/bash')
        self.assertRaises(bs.InconsistentUserData, mgr.get_user, 'user1')
        self.assertRaises(bs.InconsistentUserData, mgr.get_user, 'user2')
        u3 = mgr.get_user('user3')
        self.assertIsNone(u3.password_hash)
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user4')
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user5')
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user5')
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user6')
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user7')
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user8')
        self.assertRaises(bs.InvalidData, mgr.get_user, 'user9')
        u10 = mgr.get_user('user10')
        self.assertEqual(u10.password_hash, '')
        u11 = mgr.get_user('user11')
        self.assertIsNone(u11.password_hash)
        u12 = mgr.get_user('user12')
        self.assertIsNone(u12.password_hash)
        u13 = mgr.get_user('user13')
        self.assertEqual(u13.gecos, '')
        u14 = mgr.get_user('user14')
        self.assertIsNone(u14.password_hash)
        u15 = mgr.get_user('user15')
        self.assertEqual(u15.password_hash, '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0')
        self.assertEqual(mgr.get_user('nosuchuser'), None)

    def test_get_user_by_uid(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            user0:x:1000:1000:Test User,,,:/home/user0:/bin/bash
            user1:x:1001:1001:Test User,,,:/home/user1:/bin/false
            user2:x:1001:1002:Test User,,,:/home/user2:/bin/bash
            user3:!:1003:1003:Test User,,,:/home/user3:/bin/bash
            user4:*:1004:1004:Test User,,,:/home/user4:/bin/bash
            user5::1005:1005:Test User,,,:/home/user5:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            user0:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:16956:0:99999:7:::
            user1:!:18354:0:99999:7:::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        u0 = mgr.get_user_by_uid(1000)
        self.assertIsInstance(u0, bs.User)
        self.assertEqual(u0.uid, 1000)
        self.assertEqual(u0.name, 'user0')
        self.assertRaises(bs.IdConflict, mgr.get_user_by_uid, 1001)
        u3 = mgr.get_user_by_uid(1003)
        self.assertIsInstance(u3, bs.User)
        u4 = mgr.get_user_by_uid(1004)
        self.assertIsInstance(u4, bs.User)
        u5 = mgr.get_user_by_uid(1005)
        self.assertIsInstance(u5, bs.User)


    def test_get_group(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            user0:x:1000:
            group012:x:9000:user1,user0,user2
            group123:x:9000:user1,user2,user3
            group123:x:9000:user0,user3,user4
            badgroup:xxx:
            badgid:x:gid:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(""" 
            user0:!::
            group012:!::
            group123:!::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            user0:x:1000:1000:User,,,:/home/user0:/bin/bash
            user1:x:1001:1001:User,,,:/home/user1:/bin/false
            user2:x:1002:1002:User,,,:/home/user2:/bin/bash
            user3:x:1003:1003:User,,,:/home/user3:/bin/false
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            user0:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:16956:0:99999:7:::
            user1:!:18354:0:99999:7:::
            user2::16956:0:99999:7:::
            user3:!:18354:0:99999:7:::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        g0 = mgr.get_group('user0')
        self.assertIsInstance(g0, bs.Group)
        self.assertEqual(g0.name, 'user0')
        self.assertEqual(g0.gid, 1000)
        self.assertEqual(g0.users, {'user0'})
        g012 = mgr.get_group('group012')
        self.assertEqual(g012.users, {'user0', 'user1', 'user2'})
        self.assertRaises(bs.InconsistentGroupData, mgr.get_group, 'group123')
        self.assertRaises(bs.InvalidData, mgr.get_group, 'badgroup')
        self.assertRaises(bs.InvalidData, mgr.get_group, 'badgid')

    def test_get_group_by_gid(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
                group0:x:1000:
                group1:x:1001:
                group2:x:1001:
                group3:!:1003:
                group4:*:1004:
                group5::1005:
            """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(""" 
                group0:!::
                group1:!::
                group2:!::
            """).lstrip())
        mgr = bs.LinuxUserManager()
        g0 = mgr.get_group_by_gid(1000)
        self.assertIsInstance(g0, bs.Group)
        self.assertEqual(g0.gid, 1000)
        self.assertEqual(g0.name, 'group0')
        self.assertRaises(bs.IdConflict, mgr.get_group_by_gid, 1001)
        g3 = mgr.get_group_by_gid(1003)
        self.assertIsInstance(g3, bs.Group)
        g4 = mgr.get_group_by_gid(1004)
        self.assertIsInstance(g4, bs.Group)
        g5 = mgr.get_group_by_gid(1005)
        self.assertIsInstance(g5, bs.Group)

    def test_create_group(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            u185128:x:34450:
            u185130:x:29411:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(""" 
            u185128:!::
            u185130:!::
        """).lstrip())
        bs.LinuxUserManager().create_group('u185131', gid=50700)
        etc_group = self.fs.get_object('/nowhere/etc/group')
        self.assertEqual(etc_group.stat_result.st_mode, 0o100644)
        self.assertEqual(etc_group.contents, dedent("""
            u185128:x:34450:
            u185130:x:29411:
            u185131:x:50700:
        """).lstrip())
        etc_gshadow = self.fs.get_object('/nowhere/etc/gshadow')
        self.assertEqual(etc_gshadow.stat_result.st_mode, 0o100640)
        self.assertEqual(etc_gshadow.contents, dedent(""" 
            u185128:!::
            u185130:!::
            u185131:!::
        """).lstrip())

    def test_create_group_existing(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            u185128:x:34450:
            u185131:x:50700:
            u185130:x:29411:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(""" 
            u185128:!::
            u185131:!::
            u185130:!::
        """).lstrip())
        bs.LinuxUserManager().create_group('u185131', gid=50700)
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u185128:x:34450:
            u185131:x:50700:
            u185130:x:29411:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent(""" 
            u185128:!::
            u185131:!::
            u185130:!::
        """).lstrip())

    def test_create_group_existing_with_members(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            group0:x:1000:user0,user1,user2
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents='group0:!::user0,user1,user2')
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
                user0:x:1000:1000:Test User,,,:/home/user0:/bin/bash
                user1:x:1001:1001:Test User,,,:/home/user1:/bin/false
                user2:x:1002:1002:Test User,,,:/home/user2:/bin/bash
            """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
                user0:!:18354:0:99999:7:::
                user1::16956:0:99999:7:::
                user2:!:18354:0:99999:7:::
            """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.create_group('group0', 1000)
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            group0:x:1000:user0,user1,user2
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, 'group0:!::user0,user1,user2')

    def test_create_group_existing_multiple(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
                u185128:x:34450:
                u185131:x:50700:
                u185130:x:29411:
                u185131:x:99999:
            """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(""" 
                u185131:!::
                u185128:!::
                u185131:!::
                u185131:!::
                u185130:!::
            """).lstrip())
        bs.LinuxUserManager().create_group('u185131', gid=50700)
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
                u185128:x:34450:
                u185130:x:29411:
                u185131:x:50700:
            """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent(""" 
                u185128:!::
                u185130:!::
                u185131:!::
            """).lstrip())

    def test_create_group_existing_malformed(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            hosting_accounts:x::
            u185131:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(""" 
            hosting_accounts:!::
            u185131:!::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.create_group('u185131', gid=50700)
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            hosting_accounts:x::
            u185131:x:50700:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent(""" 
            hosting_accounts:!::
            u185131:!::
        """).lstrip())
        mgr.create_group('hosting_accounts')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u185131:x:50700:
            hosting_accounts:x:1:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent(""" 
            u185131:!::
            hosting_accounts:!::
        """).lstrip())

    def test_create_group_first_time(self):
        bs.LinuxUserManager().create_group('testgroup', gid=1000)
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, 'testgroup:x:1000:\n')
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, 'testgroup:!::\n')

    def test_create_group_empty_files(self):
        for each in ('/nowhere/etc/group', '/nowhere/etc/gshadow'): self.fs.create_file(each)
        bs.LinuxUserManager().create_group('testgroup', gid=1000)
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, 'testgroup:x:1000:\n')
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, 'testgroup:!::\n')

    def test_create_group_conflicting_gid(self):
        self.fs.create_file('/nowhere/etc/group', contents=dedent("""
            u185128:x:34450:
            u185130:x:29411:
        """).lstrip())
        self.assertRaises(bs.IdConflict, bs.LinuxUserManager().create_group, 'u185131', gid=29411)

    def test_create_group_no_gid(self):
        CONFIG.builtinservice.linux_user_manager.min_uid = 1000
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            group3:x:4:
            group0:x:1:
            group2:x:3:
            groupplusinf:x:1000:
            group1:x:2:
        """).lstrip())
        bs.LinuxUserManager().create_group('group4')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent(f"""
            group3:x:4:
            group0:x:1:
            group2:x:3:
            groupplusinf:x:1000:
            group1:x:2:
            group4:x:5:
        """).lstrip())

    def test_create_group_existing_no_gid(self):
        self.fs.create_file('/nowhere/etc/group', contents='testgroup:x:123123:')
        bs.LinuxUserManager().create_group('testgroup')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, 'testgroup:x:123123:')

    def test_create_group_empty_name(self):
        self.assertRaises(bs.InconsistentGroupData, bs.LinuxUserManager().create_group, '')

    def test_add_user_to_group(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80742:80742:account:/home/u223136:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:!:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            group0:x:1000:u223136
            group1:x:1001:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            group0:!::u223136
            group1:!::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.add_user_to_group('u223135', 'group1')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            group0:x:1000:u223136
            group1:x:1001:u223135
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            group0:!::u223136
            group1:!::u223135
        """).lstrip())
        mgr.add_user_to_group('u223135', 'group0')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            group0:x:1000:u223135,u223136
            group1:x:1001:u223135
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            group0:!::u223135,u223136
            group1:!::u223135
        """).lstrip())
        mgr.add_user_to_group('u223135', 'group0')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
                group0:x:1000:u223135,u223136
                group1:x:1001:u223135
            """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
                group0:!::u223135,u223136
                group1:!::u223135
            """).lstrip())
        self.assertRaises(bs.InconsistentUserData, mgr.add_user_to_group, 'nosuchuser', 'group0')
        self.assertRaises(bs.InconsistentGroupData, mgr.add_user_to_group, 'nosuchuser', 'nosuchgroup')
        self.assertRaises(bs.InconsistentGroupData, mgr.add_user_to_group, 'u223135', 'nosuchgroup')

    def test_remove_user_from_group(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80742:80742:account:/home/u223136:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:!:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            group0:x:1000:u223136,u223135
            group1:x:1001:u223135
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            group0:!::u223136,u223135
            group1:!::u223135
        """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.remove_user_from_group('u223135', 'group1')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            group0:x:1000:u223136,u223135
            group1:x:1001:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            group0:!::u223136,u223135
            group1:!::
        """).lstrip())
        mgr.remove_user_from_group('u223135', 'group0')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            group0:x:1000:u223136
            group1:x:1001:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            group0:!::u223136
            group1:!::
        """).lstrip())
        mgr.remove_user_from_group('u223135', 'group0')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            group0:x:1000:u223136
            group1:x:1001:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            group0:!::u223136
            group1:!::
        """).lstrip())
        self.assertRaises(bs.InconsistentGroupData, mgr.remove_user_from_group, 'u223135', 'nosuchgroup')

    @patch('time.time')
    def test_create_user(self, mock_time):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
            u223137:x:80744:80744:account:/home/u223137:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
            u223137:$1$5CHqbhOE$wEtod/g2KhiaZbbuPEWc4.:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            u223135:x:80742:
            u223136:x:80743:
            u223137:x:80744:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            u223135:!::
            u223136:!::
            u223137:!::
        """).lstrip())
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u2000', 2000, '/home/u2000', '$1$0VRjGj9n$kn6G7fJxy9ZA8Gw68cVOy.', '/bin/bash', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
            u223137:x:80744:80744:account:/home/u223137:/bin/bash
            u2000:x:2000:2000:account:/home/u2000:/bin/bash
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
            u223137:$1$5CHqbhOE$wEtod/g2KhiaZbbuPEWc4.:18224:0:99999:7:::
            u2000:$1$0VRjGj9n$kn6G7fJxy9ZA8Gw68cVOy.:18355:0:99999:7:::
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u223135:x:80742:
            u223136:x:80743:
            u223137:x:80744:
            u2000:x:2000:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            u223135:!::
            u223136:!::
            u223137:!::
            u2000:!::
        """).lstrip())
        home = self.fs.get_object('/home/u2000')
        self.assertEqual(home.st_mode, 0o40700)
        self.assertEqual(home.st_uid, 2000)
        self.assertEqual(home.st_gid, 2000)

    def test_create_user_existing(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            u223135:!::
            u223136:!::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.create_user('u223135', 80742, '/home/u223135', '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0', '/bin/bash', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            u223135:!::
            u223136:!::
        """).lstrip())

    @patch('time.time')
    def test_create_user_existing_multiple(self, mock_time):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223135:x:80741:80741:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            u223135:!::
            u223136:!::
        """).lstrip())
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u223135', 80742, '/home/u223135', '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0', '/bin/bash', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18355:0:99999:7:::
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            u223135:!::
            u223136:!::
        """).lstrip())

    @patch('time.time')
    def test_create_user_existing_malformed(self, mock_time):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:24:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            u223135:!::
            u223136:!::
        """).lstrip())
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u223135', 80742, '/home/u223135', '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0', '/bin/bash', 'account')
        mgr.create_user('u223136', 80743, '/home/u223136', '$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/', '/nologin', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/nologin
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18355:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18355:0:99999:7:::
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            u223135:!::
            u223136:!::
        """).lstrip())

    @patch('time.time')
    def test_create_user_first_time(self, mock_time):
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u223135', 80742, '/home/u223135', '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0', '/bin/bash', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18355:0:99999:7:::
        """).lstrip())

    @patch('time.time')
    def test_create_user_empty_files(self, mock_time):
        for each in ('/nowhere/etc/passwd', '/nowhere/etc/shadow'): self.fs.create_file(each)
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u223135', 80742, '/home/u223135', '$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0', '/bin/bash', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18355:0:99999:7:::
        """).lstrip())

    @patch('time.time')
    def test_create_user_with_extra_groups(self, mock_time):
        CONFIG.builtinservice.linux_user_manager.min_uid = 2000
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            u223135:x:80742:
            group0:x:1000:u223135
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            u223135:!::
            group0:!::u223135
        """).lstrip())
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u2000', 2000, '/home/u2000', '$1$0VRjGj9n$kn6G7fJxy9ZA8Gw68cVOy.', '/bin/bash', 'account',
                        extra_groups=('group0', 'group1'))
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u2000:x:2000:2000:account:/home/u2000:/bin/bash
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u2000:$1$0VRjGj9n$kn6G7fJxy9ZA8Gw68cVOy.:18355:0:99999:7:::
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u223135:x:80742:
            group0:x:1000:u2000,u223135
            u2000:x:2000:
            group1:x:1:u2000
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            u223135:!::
            group0:!::u2000,u223135
            u2000:!::
            group1:!::u2000
        """).lstrip())

    def test_create_user_empty_name(self):
        self.assertRaises(bs.InconsistentUserData,
                          bs.LinuxUserManager().create_user, '', 1, 'rest', 'does', 'not', 'matter')

    @patch('time.time')
    def test_create_user_without_password(self, mock_time):
        mock_time.return_value = 1585905284.8418486
        mgr = bs.LinuxUserManager()
        mgr.create_user('u223135', 80742, '/home/u223135', None, '/bin/bash', 'account')
        mgr.create_user('u223136', 80743, '/home/u223136', '', '/bin/bash', 'account')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/bin/bash
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            u223135:*:18355:0:99999:7:::
            u223136:*:18355:0:99999:7:::
        """).lstrip())


    def test_delete_user(self):
        self.fs.create_dir('/home/user0')
        self.fs.create_dir('/home/user1')
        self.fs.create_dir('/home/user2')
        self.fs.create_dir('/home/user3')
        self.fs.create_dir('/home/user4')
        self.fs.create_dir('/home/user5')
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            user0:x:1000:1000:Test User,,,:/home/user0:/bin/bash
            user1:x:1001:1001:Test User,,,:/home/user1:/bin/false
            user1:x:9999:9999:Test User,,,:/home/user1:/bin/false
            user2:x:1002:1002:Test User,,,:/home/user2:/bin/bash
            user3:x:1003:1003:Test User,,,:/home/user3:/bin/bash
            user4:x:1004:1004:Test User,,,:/home/user4:/bin/bash
            user5:m:e:s:s:e:d:u:p
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            user0:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:16956:0:99999:7:::
            user1:!:18354:0:99999:7:::
            user3:!:18354:0:99999:7:::
            user4:w:t:f:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            user3:x:1003:
            user0:x:1000:
            user2:x:2333:
            user1:x:1001:
            group03:x:9000:user0,user3
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            user3:!::
            user0:!::
            user2:!::
            user1:!::
            group03:!::user0,user3
        """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.delete_user('user0')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            user1:x:1001:1001:Test User,,,:/home/user1:/bin/false
            user1:x:9999:9999:Test User,,,:/home/user1:/bin/false
            user2:x:1002:1002:Test User,,,:/home/user2:/bin/bash
            user3:x:1003:1003:Test User,,,:/home/user3:/bin/bash
            user4:x:1004:1004:Test User,,,:/home/user4:/bin/bash
            user5:m:e:s:s:e:d:u:p
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, dedent("""
            user1:!:18354:0:99999:7:::
            user3:!:18354:0:99999:7:::
            user4:w:t:f:
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            user3:x:1003:
            user2:x:2333:
            user1:x:1001:
            group03:x:9000:user3
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, dedent("""
            user3:!::
            user2:!::
            user1:!::
            group03:!::user3
        """).lstrip())
        mgr.delete_user('user1')
        mgr.delete_user('user2')
        mgr.delete_user('user3')
        mgr.delete_user('user4')
        mgr.delete_user('user5')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, '')
        self.assertEqual(self.fs.get_object('/nowhere/etc/shadow').contents, '')
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, 'group03:x:9000:\n')
        self.assertEqual(self.fs.get_object('/nowhere/etc/gshadow').contents, 'group03:!::\n')
        self.assertRaises(OSError, self.fs.get_object, '/home/user0')
        self.assertRaises(OSError, self.fs.get_object, '/home/user1')
        self.assertRaises(OSError, self.fs.get_object, '/home/user2')
        self.assertRaises(OSError, self.fs.get_object, '/home/user3')
        self.assertRaises(OSError, self.fs.get_object, '/home/user4')
        self.assertRaises(OSError, self.fs.get_object, '/home/user5')

    @patch('os.environ', autospec=True)
    @patch('subprocess.Popen')
    def test_set_quota(self, mock_popen, mock_env):
        mock_env.get.return_value = None
        mock_popen.return_value.returncode = 0
        mock_popen.return_value.communicate.return_value = (b'', b'')
        bs.LinuxUserManager().set_quota(2000, 10485760)
        mock_popen.assert_called_once_with('setquota -g 2000 0 10240 0 0 /home',
                                           executable='/bin/bash',
                                           shell=True,
                                           stderr=-1,
                                           stdin=-1,
                                           stdout=-1,
                                           env={'PATH': None, 'SSL_CERT_FILE': None})

    @patch('subprocess.Popen')
    def test_get_quota(self, mock_popen):
        mock_popen.return_value.returncode = 0
        mock_popen.return_value.communicate.return_value = (dedent("""
            *** Report for group quotas on device /dev/sda4
            Block grace time: 7days; Inode grace time: 7days
                                    Block limits                File limits
            Group           used    soft    hard  grace    used  soft  hard  grace
            ----------------------------------------------------------------------
            #0        -- 180951028       0       0      0  469504     0     0      0
            #38191    --   19056       0       0      0    1363     0     0      0
            #7888     --  386536       0       0      0   10075     0     0      0
            #60610    -- 1162920       0 10485760      0   35741     0     0      0
            #11832    --       8       0       0      0       3     0     0      0
            #78847    --      24       0       0      0       6     0     0      0
        """).lstrip().encode(), b'')
        quota = bs.LinuxUserManager().get_quota()
        self.assertIsInstance(quota, dict)
        self.assertEqual(quota.get(0), 185293852672)
        self.assertEqual(quota.get(38191), 19513344)
        self.assertEqual(quota.get(7888), 395812864)
        self.assertEqual(quota.get(60610), 1190830080)
        self.assertEqual(quota.get(11832), 8192)
        self.assertEqual(quota.get(78847), 24576)

    def test_get_cpuacct(self):
        CONFIG.builtinservice.cgroupfs_mountpoint = '/sys/fs/cgroup'
        CONFIG.builtinservice.linux_user_manager.limitgroup = 'limitgroup'
        self.fs.create_file('/sys/fs/cgroup/cpuacct/limitgroup/u2000/cpuacct.usage', contents='151714983162')
        mgr = bs.LinuxUserManager()
        self.assertEqual(mgr.get_cpuacct('u2000'), 151714983162)
        self.assertEqual(mgr.get_cpuacct('u3000'), 0)

    def test_create_authorized_keys(self):
        bs.LinuxUserManager().create_authorized_keys(dedent("""
            ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDCt2QOfR8hS3/7aH0iWs7YYcdkwpZvUfdr1LpZWTcP9vZ+PCAi3ZWOPYJzUpUF+1yMBGSuB1nnpCD69XFfqGOpX3odIFcxvCien3EHZPGYS3jDqmRXLMI/uhJQVjlWoILeTFWJMtENsYxGoUr2V6+k0cyzPbt1fDpTrx+GbCUAjD+dBEfTBeMTnxaS9GKl7ZucbcoSYJDoKP3ladOH7giXZzZFpgLfUGfNwpjBfz/PFumx9r1IUnGXEQGYIswLr8sB/cEm1uJnCcPCC1DHPaPoQuXf8YjhpulUYFesBDO+AIFABrdIjV+MZL4zE3HktKahBHSD1EwzXg5/9UYNAY7Z
        """).lstrip(), 2000, '/home/u2000')
        authorized_keys = self.fs.get_object('/home/u2000/.ssh/authorized_keys')
        self.assertEqual(authorized_keys.st_mode, 0o100400)
        self.assertEqual(authorized_keys.st_uid, 2000)
        self.assertEqual(authorized_keys.st_gid, 2000)
        self.assertEqual(authorized_keys.contents, dedent("""
            ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDCt2QOfR8hS3/7aH0iWs7YYcdkwpZvUfdr1LpZWTcP9vZ+PCAi3ZWOPYJzUpUF+1yMBGSuB1nnpCD69XFfqGOpX3odIFcxvCien3EHZPGYS3jDqmRXLMI/uhJQVjlWoILeTFWJMtENsYxGoUr2V6+k0cyzPbt1fDpTrx+GbCUAjD+dBEfTBeMTnxaS9GKl7ZucbcoSYJDoKP3ladOH7giXZzZFpgLfUGfNwpjBfz/PFumx9r1IUnGXEQGYIswLr8sB/cEm1uJnCcPCC1DHPaPoQuXf8YjhpulUYFesBDO+AIFABrdIjV+MZL4zE3HktKahBHSD1EwzXg5/9UYNAY7Z
        """).lstrip())

    @patch('psutil.process_iter', autospec=True)
    def test_kill_user_processes(self, mock_process_iter):
        process1 = Mock(spec=psutil.Process)
        process1.uids.return_value = (1000, 1000, 1000)
        process2 = Mock(spec=psutil.Process)
        process2.uids.return_value = (1000, 1000, 1000)
        process3 = Mock(spec=psutil.Process)
        process3.uids.return_value = (0, 0, 0)
        mock_process_iter.return_value = (p for p in (process1, process2, process3))
        self.fs.create_file('/nowhere/etc/passwd', contents='user:x:1000:1000:Test User,,,:/home/user:/bin/bash')
        self.fs.create_file('/nowhere/etc/shadow', contents='user:$1$aRDLQJXb$TXKgBfCWPOjFiMWfBXOW0:16956:0:99999:7:::')
        bs.LinuxUserManager().kill_user_processes('user')
        process1.terminate.assert_called_once()
        process2.terminate.assert_called_once()
        process3.terminate.assert_not_called()
        mock_process_iter.return_value = (p for p in (process1, process2, process3))
        bs.LinuxUserManager().kill_user_processes('nobody')

    def test_set_shell(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
        """).lstrip())
        bs.LinuxUserManager().set_shell('u223135', '/bin/zsh')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/zsh
        """).lstrip())

    def test_set_comment(self):
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
                u223135:x:80742:80742:account:/home/u223135:/bin/bash
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
                u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
        """).lstrip())
        bs.LinuxUserManager().set_comment('u223135', 'Hosting account,,,')
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
                u223135:x:80742:80742:Hosting account,,,:/home/u223135:/bin/bash
        """).lstrip())

    @patch('os.environ', autospec=True)
    @patch('subprocess.Popen')
    def test_change_uid(self, mock_popen, mock_env):
        mock_env.get.return_value = None
        mock_popen.return_value.returncode = 0
        mock_popen.return_value.communicate.return_value = (b'', b'')
        self.fs.create_file('/nowhere/etc/passwd', contents=dedent("""
            u223135:x:80742:80742:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
        """).lstrip())
        self.fs.create_file('/nowhere/etc/shadow', contents=dedent("""
            u223135:$1$aRDLQJXb$TXKgBfCWPOKjFiMWfBXOW0:18224:0:99999:7:::
            u223136:$1$50i6mh7B$d7XLzlLdt0eAXaDPFtHwH/:18224:0:99999:7:::
        """).lstrip())
        self.fs.create_file('/nowhere/etc/group', contents=dedent(f"""
            u223135:x:80742:
            u223136:x:80743:
        """).lstrip())
        self.fs.create_file('/nowhere/etc/gshadow', contents=dedent(f"""
            u223135:!::
            u223136:!::
        """).lstrip())
        mgr = bs.LinuxUserManager()
        mgr.change_uid('u223135', 2000)
        self.assertEqual(self.fs.get_object('/nowhere/etc/passwd').contents, dedent("""
            u223135:x:2000:2000:account:/home/u223135:/bin/bash
            u223136:x:80743:80743:account:/home/u223136:/usr/sbin/nologin
        """).lstrip())
        self.assertEqual(self.fs.get_object('/nowhere/etc/group').contents, dedent("""
            u223135:x:2000:
            u223136:x:80743:
        """).lstrip())
        mock_popen.assert_called_once_with('chown -R 2000:2000 /home/u223135',
                                           executable='/bin/bash',
                                           shell=True,
                                           stderr=-1,
                                           stdin=-1,
                                           stdout=-1,
                                           env={'PATH': None, 'SSL_CERT_FILE': None})
        self.assertRaises(bs.IdConflict, mgr.change_uid, 'u223136', 2000)
