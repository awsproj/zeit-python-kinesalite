#!/usr/bin/env python
#  kinesis_data_client.py

# https://aws.amazon.com/blogs/big-data/snakes-in-the-stream-feeding-and-eating-amazon-kinesis-streams-with-python/
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

import boto3
import pprint
import time

pp = pprint.PrettyPrinter(indent=4)

class KinesisDataClient:

    class Logger:
        def __init__(self, debug=False, print_to_log=False, pformat=False, outputs={}):
            self._debug = debug
            self._print_to_log = print_to_log
            self._pformat = pformat
            self._outputs = outputs
        def __call__(self, *args, **kwargs):
            output_content = self._outputs.get("content", None)
            otyp = type(output_content)
            if not (otyp in (unicode, str)):
                self._outputs["content"] = ""
            if self._pformat:
                self._logpp(*args, **kwargs)
            else:
                self._log(*args, **kwargs)
        def _log(self, msg):
            if not self._debug:
                return
            if self._print_to_log:
                self._outputs["content"] += msg + "\n"
            else:
                print(msg)
        def _logpp(self, msg):
            if not self._debug:
                return
            if self._print_to_log:
                self._outputs["content"] += pp.pformat(msg)
            else:
                pp.pprint(msg)

    def _init_log(self):
        self._debug = True
        self._print_to_log = False
        self._log_outputs = {"content": ""}

        self._log = KinesisDataClient.Logger(debug=self._debug, print_to_log=self._print_to_log)
        self._logpp = KinesisDataClient.Logger(debug=self._debug, print_to_log=self._print_to_log,
                                               pformat=True)


    def __init__(self, endpoint_url='http://localhost:4567', stream_name=None):
        self._init_log()
        self._endpoint_url = endpoint_url
        self._kinesis = boto3.client('kinesis', endpoint_url=endpoint_url, # default None
                                     #three kwargs needed by boto client if not already
                                     #set through environment. when connecting to aws
                                     #it is preferred to set them through environment.
                                     #when connecting to local kinesalite server, use these dummies.
                                       region_name='dummy-region', #default None
                                       aws_access_key_id='dummy-key-id', # default None
                                       aws_secret_access_key='dummy-secret', # default None
                                     #other parameters
                                       #api_version=None,
                                       #use_ssl=False, # default True,
                                       #verify=None,
                                       #aws_session_token=None, config=None
                                     )
        self._stream_name = stream_name
        self._rec_list = {"ok_list":[], "ok_list_tstamp":0, "failed_list":[]}
        self._rec_shard = {"ok_list":[], "ok_list_tstamp":0, "failed_list":[]}

    def op_list(self, headers_only=False):

        # create and check stream
        self._log('status endpoint %s stream_name %s' % (
            str(self._endpoint_url), str(self._stream_name)))

        # list streams
        stream_lists = []
        stream_lists_err = None
        stream_ts_sum = 0
        while True:
            stream_ts0 = time.time()
            stream_list = self._kinesis.list_streams(
                Limit=10, #Limit (integer) -- The maximum number of streams to list.
                #ExclusiveStartStreamName (string) -- The name of the stream to start the list with.
            )
            stream_ts1 = time.time()
            stream_ts_sum += stream_ts1 - stream_ts0
            self._log("streams")
            self._logpp(stream_list)

            s_has_more = False
            s_list = None
            s_http_res_meta = stream_list.get("ResponseMetadata", None)
            s_http_res_stt = 0
            if s_http_res_meta is not None:
                s_http_res_stt = s_http_res_meta.get("HTTPStatusCode", 0)
            if s_http_res_stt == 200:
                s_list = stream_list.get("StreamNames", None)
                if s_list is not None:
                    stream_lists.extend(s_list)
                    s_has_more = stream_list.get("HasMoreStreams", None)
            if s_has_more is None or s_list is None or s_list is None:
                stream_lists_err = True
            if not s_has_more:
                break
        tstamp = time.time()
        if not stream_lists_err:
            self._rec_list["ok_list"] = stream_lists
            self._rec_list["ok_list_tstamp"] = tstamp
        else:
            self._rec_list["faile"].extend({"failed_tstamp": tstamp, "failed_op": "stream_list"})

        # list shard
        shard_lists = []
        shard_ts_sum = 0
        if not stream_lists_err and len(stream_lists) > 0:
            stream_name = self._stream_name
            shard_list = {'Shards':[]}
            if stream_name in stream_lists:
                shard_ts0 = time.time()
                shard_list = self._kinesis.list_shards(StreamName=stream_name, MaxResults=4)
                shard_ts1 = time.time()
                shard_ts_sum += shard_ts1 - shard_ts0
            self._log("shards")
            self._logpp(shard_list)

            if len(shard_list['Shards']) == 1:
                shard_id = shard_list['Shards'][0]['ShardId']
                shard_lists.extend(shard_id)

                shard_it = self._kinesis.get_shard_iterator(StreamName=stream_name,
                                                      ShardId= shard_id, #'shardId-000000000000',
                                                      #ShardId='shardId-000000000000',
                                                      ShardIteratorType='TRIM_HORIZON')["ShardIterator"]
                loopcnt = 0
                last_ago = 0.0
                max_dif = 0.0
                rec_sums = 0
                while loopcnt < 4000:
                    break
                    out = self._kinesis.get_records(ShardIterator=shard_it, Limit=20)
                    shard_it = out["NextShardIterator"]
                    retv_ago = out['MillisBehindLatest']
                    retv_ago_sec, retv_ago_ms = int(retv_ago/1000), int(retv_ago % 1000)
                    dif_ago = last_ago - retv_ago
                    if dif_ago > max_dif: max_dif = dif_ago
                    self._log("\n%s%s%s" % ( "Out for %d.%03d ago" % (retv_ago_sec, retv_ago_ms),
                                         " %.2f hours diff %.2f sec max %.2f" % (
                                             retv_ago_sec / 3600.0, dif_ago, max_dif ),
                                         " loopcnt %d" % loopcnt ))
                    last_ago = retv_ago
                    #self._logpp(out)
                    if len(out['Records']) < 1 and retv_ago_sec == 0 and retv_ago_ms == 0:
                        break
                    xlen = len(out['Records'])
                    if xlen > 0:
                        rec_sums += xlen
                        self._log("  records xlen %d" % xlen)
                        #continue
                    for x in out['Records']:
                        self.logpp(x)
                    loopcnt += 1

                self._log("  rec_sums %d" % rec_sums)
        tstamp = time.time()
        if len(shard_lists) == 1:
            self._rec_shard["ok_list"] = shard_lists
            self._rec_shard["ok_list_tstamp"] = tstamp
        else:
            self._rec_shard["failed_list"].append({"failed_tstamp": tstamp, "failed_op": "shard_list"})

        retv = "_rec_list %s  _rec_shard %s  costs %.3f %.3f" % (
            str(self._rec_list), str(self._rec_shard), stream_ts_sum, shard_ts_sum)
        self._log(retv)

        if len(self._log_outputs["content"]) > 0:
            retv += "\n\n" + self._log_outputs["content"]
            self._log_outputs["content"] = ""

        return retv

    def op_put(self, data=""):

        # put data to the stream
        ts_put_0 = time.time()
        status_put_ok = False
        try:
            if len(self._stream_name) > 3:
                self._log("op putrecord stream_name " + str(self._stream_name))
                self._kinesis.put_record(StreamName=self._stream_name,
                                         Data=data,
                                         PartitionKey="partitionkey")
                status_put_ok = True
            else:
                self._log("Error: putrecord stream_name len 3 or less")
        except BaseException as e:
            self._log("Error: putrecord stream_name except " + str(self._stream_name))
            self._log("Exception " + str(e))
        ts_put_1 = time.time()
        lmsg = "putrecord ok %s time_cost %.3f" % (
            str(status_put_ok), ts_put_1 - ts_put_0)
        self._log(lmsg)
        return lmsg

    def op_create(self):
        # create and check stream
        ts_create_0 = time.time()
        status_create_ok = False
        try:
            if len(self._stream_name) > 3:
                self._log("op create stream_name " + str(self._stream_name))
                self._kinesis.create_stream(StreamName=self._stream_name, ShardCount=1)  # no retv
                status_create_ok = True
            else:
                self._log("Error: create stream_name len 3 or less")
        except BaseException as e:
            self._log("Error: create stream_name except " + str(self._stream_name))
            self._log("Exception " + str(e))
        ts_create_1 = time.time()
        lmsg = "create ok %s time_cost %.3f" % (
            str(status_create_ok), ts_create_1 - ts_create_0)
        self._log(lmsg)
        return lmsg

