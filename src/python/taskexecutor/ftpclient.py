import io
import ftplib
import os


class FTPClient:
    def __init__(self, host, user, password):
        self.host = host
        self._user = user
        self._password = password
        self._server = None
        self._connect()

    def _connect(self):
        self._server = ftplib.FTP(self.host)
        self._server.login(self._user, self._password)
        self._server.encoding = 'utf-8'

    def _test_connection(self):
        self._connect()

    def _check_dir(self, dirname):
        filelist = []
        self._server.retrlines('LIST', filelist.append)
        found = False
        for f in filelist:
            if f.split()[-1] == dirname and f.lower().startswith('d'):
                found = True
        if not found:
            self._server.mkd(dirname)
        self._server.cwd(dirname)


    def upload(self, file, remote_filename):
        self._test_connection()
        dirpath, filename = os.path.split(remote_filename)
        for d in dirpath.split('/'):
            if d != '':
                self._check_dir(d)
        if not isinstance(file, io.BufferedIOBase):
            file = open(file, "rb")
        with file as f:
            self._server.storbinary("STOR {}".format(filename), f)
        self._server.cwd("/")

    def delete(self, remote_filename):
        self._test_connection()
        dirpath, filename = os.path.split(remote_filename)
        try:
            self._server.cwd(dirpath)
            self._server.delete(filename)
            dirs = [d for d in dirpath.split('/') if d != '']
            while dirs:
                self._server.cwd("..")
                self._server.rmd(dirs.pop())
        except ftplib.error_perm:
            pass
        self._server.cwd("/")
