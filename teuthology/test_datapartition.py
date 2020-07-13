# Copyright 2020 The ChubaoFS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: utf-8 -*-
import pytest

import env
import unittest2
import requests
import json


def print_url(url):
    print("request url:" + url)

def get_user_vol_list():
    rst = {}
    url = env.MASTER + "/user/list";
    print_url(url)
    result = requests.get(url)
    if result.status_code != 200:
        return {}
    content = json.loads(result.content.decode())
    #print(content)

    for u in content["data"]:
        uid = u["user_id"]
        for vol in u["policy"]["own_vols"]:
            #print(vol + " " + uid)
            rst[vol] = uid
    return rst

def get_replica_by_dpid(dpid):
    replica_addrs = {}
    url = env.MASTER + "/dataPartition/get?id=" + str(dpid);
    print_url(url)
    result = requests.get(url)
    if result.status_code != 200:
        return []
    content = json.loads(result.content.decode())
    #print(content)

    for r in content["data"]["Replicas"]:
        print(r)
        replica_addrs[r["Addr"]] = r["DiskPath"]
    return replica_addrs

class DataPartitionTestCase(unittest2.TestCase):

    def assert_base_resp(self, content):
        #TODO 没有一个统一的格式
        assert 'code' in content
        #print(content['code'])
        assert(content['code'] == 0 or content['code'] == 200 or content['code'] == 303)
        assert 'msg' in content
        #assert content['msg'] == "ok"
        assert 'data' in content


    def get_client_dp(self):
        dpids = []
        rst = get_user_vol_list()
        assert len(rst) > 0

        vol = ""
        for v in rst:
            vol = v
            break

        url = env.MASTER + "/client/partitions?name=" + vol;
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        #print(content)
        assert content["data"]["DataPartitions"]

        for dp in content["data"]["DataPartitions"]:
            print(dp)
            assert "PartitionID" in dp
            assert "Status" in dp
            assert "Epoch" in dp
            #assert "IsRecover" in dp
            assert "ReplicaNum" in dp
            assert "LeaderAddr" in dp
            assert "Hosts" in dp
            dpids.append(dp["PartitionID"])

        return dpids

    def test_dp_get(self):
        rst = self.get_client_dp()
        assert len(rst) > 0

        dpid = ""
        for id in rst:
            dpid = id
            break

        url = env.MASTER + "/dataPartition/get?id=" + str(dpid);
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        #print(content)

        assert "FileInCoreMap" in content["data"] #TODO what's inside
        assert "FilesWithMissingReplica" in content["data"]
        assert "Hosts" in content["data"]
        assert "LastLoadedTime" in content["data"]
        assert "MissingNodes" in content["data"]
        assert "PartitionID" in content["data"]
        assert "Peers" in content["data"]
        assert "ReplicaNum" in content["data"]
        assert "Replicas" in content["data"]
        assert "Status" in content["data"]
        assert "VolID" in content["data"]
        assert "VolName" in content["data"]
        assert "Zones" in content["data"]

        for peer in content["data"]["Peers"]:
            #print(peer)
            assert "id" in peer
            assert "addr" in peer

        for r in content["data"]["Replicas"]:
            #print(r)
            assert "Addr" in r
            assert "DiskPath" in r
            assert "FileCount" in r
            assert "HasLoadResponse" in r
            assert "IsLeader" in r
            assert "NeedsToCompare" in r
            assert "ReportTime" in r
            assert "Status" in r
            assert "TotalSize" in r
            assert "UsedSize" in r

    def test_dp_create(self):
        rst = get_user_vol_list()
        assert len(rst) > 0

        vol = ""
        for v in rst:
            vol = v
            break

        url = env.MASTER + "/dataPartition/create?count=3&name=" + vol
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        print(content)

    def test_dp_load(self):
        rst = self.get_client_dp()
        assert len(rst) > 0

        dpid = ""
        for id in rst:
            dpid = id
            break

        url = env.MASTER + "/dataPartition/load?id=" + str(dpid);
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        print(content)

    def test_dp_decommission(self):
        rst = self.get_client_dp()
        assert len(rst) > 0

        dpid = ""
        for id in rst:
            dpid = id
            break

        replica_addrs = get_replica_by_dpid(dpid)

        replica_addr = ""
        for key in replica_addrs:
            replica_addr = key
            break

        url = env.MASTER + "/dataPartition/decommission?id=" + str(dpid) + "&addr=" + replica_addr
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        print(content)

    def test_dp_offdisk(self):
        rst = self.get_client_dp()
        assert len(rst) > 0

        dpid = ""
        for id in rst:
            dpid = id
            break

        replica_addrs = get_replica_by_dpid(dpid)
        print(replica_addrs)

        replica_addr = ""
        disk = ""
        for key in replica_addrs:
            replica_addr = key
            disk = replica_addrs[key]
            break

        url = env.MASTER + "/disk/decommission?addr=" + replica_addr + "&disk=" + disk
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

if __name__ == '__main__':
    unittest2.main(verbosity=2)

if __name__ == "__main__":
    pytest.main(["-s", "test_datapartition.py"])
