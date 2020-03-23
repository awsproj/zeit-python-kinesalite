#!/usr/bin/env python
#  api/frontend/api_flask.py

from flask import Flask, request
import json
import os
import sys

local_base_dir = '../../www_content'
local_base_url = '/www_content'

kinesalite_endpoint = 'http://127.0.0.1:4567'
kinesalite_streamname = 'ExampleStream'

script_dir = os.path.dirname(__file__)

flask_api = Flask(__name__, static_url_path='/www_content', static_folder=local_base_dir)


def getDataBytes(index):
    import datetime, random
    data = {}
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['EVENT_TIME'] = str_now
    data['TICKER'] = random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV'])
    price = random.random() * 100
    data['PRICE'] = round(price, 2)
    data['INDEX'] = index
    d240k = "a" * 240000
    databytes = json.dumps(data) + d240k
    return databytes


companies = [{"id": 1, "name": "Company One"}, {"id": 2, "name": "Company Two"}]
contexts = { "req_count": 0, "res_content": companies}
boto_client = None
def get_api_v001(ver, req, pth, qry):
    contexts["req_count"] += 1

    import kinesis_data_client as kclient
    global boto_client
    if boto_client is None:
        boto_client = kclient.KinesisDataClient(endpoint_url=kinesalite_endpoint,
                                                stream_name=kinesalite_streamname)

    if qry == 'check/div2_content':
        contexts["res_content"] = boto_client.op_list(headers_only=True).split("\n")
    elif qry == 'check/div3_content':
        contexts["res_content"] = boto_client.op_create().split("\n")
    elif qry == 'check/div1_content':
        contexts["res_content"] = []
        import time
        ts0=time.time()
        rv1 = 0
        for idx in range(60):
            timeup = idx + ts0
            while time.time() < timeup:
                time.sleep(0.01)
                if time.time() < timeup - 1.2:
                    break
            rv1 = boto_client.op_put(data=getDataBytes(idx)).split("\n")
        ts1=time.time()
        rcost = {"put_cost": "%.3f" % (ts1-ts0)}
        contexts["res_content"].extend([rcost, rv1])
    return json.dumps(contexts)

@flask_api.route('/api/<path:ver_str>', methods=['GET'])
def get_api_v0xy(ver_str): # http://<ip>:<port>/api/v001?abcd=1234
    the_ver = ver_str # v001
    ret_val = ""
    if the_ver == 'v001':
        try: # pth: /api/v001 , qry: abcd=1234
            ret_val = get_api_v001(the_ver, request,
                                   request.path, request.query_string)
        except:
            raise
    else:
        ret_val = "unknown api version " + the_ver
    return ret_val

@flask_api.route(local_base_url + '/<path:path>', methods=['GET'])
def get_www_content(path):
    # check whether the requested path is under the local_base_dir directory
    base_dir_pth = os.path.abspath( script_dir + '/' + local_base_dir)
    thisdir = os.getcwd()
    req_pth = os.path.abspath(thisdir + local_base_url + '/' + path)
    if not req_pth.startswith(base_dir_pth):
        return "Error: requested content not in the directory tree"
    return flask_api.send_static_file(path)

@flask_api.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r

if __name__ == '__main__':
    flask_api.run()

