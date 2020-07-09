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




class MetaPartitionTestCase(unittest2.TestCase):

    def assert_base_resp(self, content):
        #TODO 没有一个统一的格式
        assert 'code' in content
        #assert content['code'] == 0
        assert 'msg' in content
        #assert content['msg'] == "ok"
        assert 'data' in content

    def assert_getpartitionbyid(self, meta_ip, pid):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getPartitionById?pid=" + pid
        print("request url:" + url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)

        data = content["data"]
        assert "cursor" in data
        assert "leaderAddr" in data
        assert "nodeId" in data
        assert "peers" in data

        peers = data["peers"]
        for peer in peers:
            assert "id" in peer
            assert "addr" in peer

    def assert_getallinodes(self, meta_ip, pid):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getAllInodes?pid=" + pid
        print("request url:" + url)
        result = requests.get(url)

        assert result.status_code == 200
        result_str = result.content.decode();
        #TODO why not return json format

        #check one node at most
        idx = result_str.find("\n")
        if idx > -1:
            oneline = result_str[0:idx]
        else:
            oneline = result_str

        line = json.loads(oneline)
        #print(line)
        assert "Flag" in line
        assert "NLink" in line
        assert "Generation" in line
        assert "LinkTarget" in line
        assert "AccessTime" in line
        assert "Reserved" in line
        assert "CreateTime" in line
        assert "Extents" in line
        assert "ModifyTime" in line
        assert "Uid" in line
        assert "Gid" in line
        assert "Size" in line
        assert "Type" in line
        assert "Inode" in line


    def assert_getalldentry(self, meta_ip, pid):
        #TODO 是否应该改为AllDentrys
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getAllDentry?pid=" + pid
        print("request url:" + url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        #print(content)
        data = content["data"]
        for n in data:
            #print(n)
            assert "Name" in n
            assert "Inode" in n
            assert "Type" in n
            assert "ParentId" in n
            #check one node at most
            break

    def assert_getpartitions(self, meta_ip):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getPartitions"
        print("request url:" + url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)

        for pid in content["data"]:
            self.assert_getpartitionbyid(meta_ip, pid)
            self.assert_getallinodes(meta_ip, pid)
            self.assert_getalldentry(meta_ip, pid)


    def test_allmetapartiions(self):
        url = env.MASTER + "/topo/get";
        print("request url:" + url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)

        assert "Zones" in content["data"];
        #print(content["data"]["Zones"])

        for zone in content["data"]["Zones"]:
            assert "Name" in zone
            print("check zone:" + zone["Name"])
            assert "Status" in zone
            if zone["Status"] == "available":
                assert "NodeSet" in zone
                for i in zone["NodeSet"]:
                    assert "MetaNodes" in zone["NodeSet"][i]
                    for node in zone["NodeSet"][i]["MetaNodes"]:
                        assert "Status" in node
                        #if node["Status"] == "True":
                        assert "Addr" in node
                        addr = node["Addr"]
                        print("check metanode:" + addr)
                        ip = addr[0:addr.find(":")]
                        self.assert_getpartitions(ip)

if __name__ == '__main__':
    unittest2.main(verbosity=2)

if __name__ == "__main__":
    pytest.main(["-s", "test_metapartition.py"])
