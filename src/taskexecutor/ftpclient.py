import io
import ftplib

class FTPClient:
    def __init__(self, host, user, password):
        self._host = host
        self._user = user
        self._password = password
        self._server = None
        self._connect()

    def _connect(self):
        self._server = ftplib.FTP(self._host)
        self._server.login(self._user, self._password)

    def _test_connection(self):
        try:
            self._server.voidcmd("NOOP")
        except ftplib.error_temp:
            self._connect()

    def upload(self, file, remote_filename):
        self._test_connection()
        if not isinstance(file, io.BufferedIOBase):
            file = open(file, "rb")
        with file as f:
            self._server.storbinary("STOR {}".format(remote_filename), f)

    def delete(self, remote_filename):
        self._test_connection()
        self._server.delete(remote_filename)



