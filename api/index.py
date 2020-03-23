#!/usr/bin/env python
# /api/index2.py
# https://zeit.co/docs/runtimes#official-runtimes/python/python-version

import sys
if sys.hexversion < 0x03050000:
    sys.exit("Python 3.5 or newer is required to run this program.")

from cowpy import cow
from urllib.parse import urlparse

try:
    import url_test_server
except:
    sys.path.insert(0, 'api')
    import url_test_server
    del sys.path[0]

class handler(url_test_server.urlTestHandler):

    c_handler_source = 'None'

    def do_GET(self):
        query_msg = ""
        do_test_server = False
        try:
            urlobj = urlparse(self.path)
            query = urlobj.query
            query_msg += str(query)
            if urlobj.path.startswith('/url_www'):
                do_test_server = True
        except:
            pass
        if len(query_msg) > 2:
            do_test_server = True
        if do_test_server:
            return url_test_server.urlTestHandler.do_GET(self)

        if len(query_msg) < 1:
            query_msg = ' query-failed '

        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.send_header('Cache-control', 'no-cache')
        self.end_headers()

        call_source = 'None'
        if type(handler.c_handler_source) is not type(None):
            if type(handler.c_handler_source) is str:
                call_source = 'type str ' + str(handler.c_handler_source)
            else:
                call_source = 'type else ' + str(type(handler.c_handler_source))
                call_source += ' ' + str(handler.c_handler_source)

        message = cow.Cowacter().milk(
            'Hello from Python from a ZEIT Now Serverless Function!' \
                                        + ' ' + call_source)

        message += ' ' + query_msg + ' '

        self.wfile.write(message.encode())
        return

if __name__ == '__main__':
    import sys
    sys.stderr.write("running in main ...\n")
    handler.c_handler_source = '__Main__'

    import os
    prog_dir = os.path.dirname(__file__)
    topdir = os.path.abspath(prog_dir + "/../")
    www_dir = os.path.join(topdir, url_test_server.local_base_path)

    server = url_test_server.urlTestServer( www_dir,
                                            url_test_server.local_base_path,
                                            ("", 8000),
                                            RequestHandlerClass=handler)
    sys.stderr.write("running on port 8000 ...\n")
    server.serve_forever()

