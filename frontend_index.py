#!/usr/bin/env python
#  frontend_index.py

import sys
#if sys.hexversion < 0x03050000:
#    sys.exit("Python 3.5 or newer is required to run this program.")

import os


from frontend.api_flask import flask_api

print(" sys.path " + str(sys.path))

if __name__ == '__main__':
    flask_api.run()

