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


class MetaPartitionTestCase(unittest2.TestCase):

    def assert_base_resp(self, content):
        #TODO 没有一个统一的格式
        assert 'code' in content
        #print(content['code'])
        assert(content['code'] == 0 or content['code'] == 200 or content['code'] == 303)
        assert 'msg' in content
        #assert content['msg'] == "ok"
        assert 'data' in content

    def assert_getpartitionbyid(self, meta_ip, pid):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getPartitionById?pid=" + pid
        print_url(url)
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

    #if ino is dir, then extent is None
    def assert_getextentsbyinode(self, meta_ip, pid, ino):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getExtentsByInode?pid=" + pid + "&ino=" + str(ino)
        print_url(url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #print(content)
        self.assert_base_resp(content)
        data = content["data"]
        assert "gen" in data
        assert "sz" in data
        assert "sz" in data
        assert "eks" in data
        eks = data["eks"]

        if eks != None:
            for ek in eks:
                assert "ExtentId" in ek
                assert "Size" in ek
                assert "CRC" in ek
                assert "ExtentOffset" in ek
                assert "PartitionId" in ek
                assert "FileOffset" in ek

    #if ino is not a directory, children is None
    def assert_getdirectory(self, meta_ip, pid, ino, typ):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getDirectory?pid=" + pid + "&parentIno=" + str(ino)
        print_url(url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        data = content["data"]
        #print(typ)
        #print(data)

        if typ == 2147484159 or typ == 2147484141:
            if data["children"] is not None:
                for child in data["children"]:
                    assert "ino" in child
                    assert "name" in child
                    assert "type" in child
        elif typ == 420:
            assert data["children"] is None
        else:
            assert False

    def assert_getallinodes(self, meta_ip, pid):
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getAllInodes?pid=" + pid
        print_url(url)
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

        if len(oneline) > 0:
            line = json.loads(oneline)
            print(line)
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

            self.assert_getextentsbyinode(meta_ip, pid, line["Inode"])

            self.assert_getdirectory(meta_ip, pid, line["Inode"], line["Type"])


    def assert_getalldentry(self, meta_ip, pid):
        #TODO 是否应该改为AllDentrys
        url = "http://" + meta_ip + ":" + env.META_PORT + "/getAllDentry?pid=" + pid
        print_url(url)
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
        print_url(url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)

        for pid in content["data"]:
            self.assert_getpartitionbyid(meta_ip, pid)
            self.assert_getallinodes(meta_ip, pid)
            self.assert_getalldentry(meta_ip, pid)
            #check one partition at most
            #break

    def test_allmetapartiions(self):
        url = env.MASTER + "/topo/get";
        print_url(url)
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
                        #check one node at most
                        #break

    def get_all_mp(self):
        print("call ---------> get_all_mp")
        rst = {}
        url = env.MASTER + "/topo/get";
        print_url(url)
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
                        meta_ip = addr[0:addr.find(":")]

                        url = "http://" + meta_ip + ":" + env.META_PORT + "/getPartitions"
                        print_url(url)
                        result = requests.get(url)

                        assert result.status_code == 200
                        content = json.loads(result.content.decode())
                        self.assert_base_resp(content)

                        for pid in content["data"]:
                            rst[pid] = addr
        print("end ---------> get_all_mp")
        return rst

    def assert_metapartition_load(self, pid):
        url = env.MASTER + "/metaPartition/load?id=" + pid
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        #print(content)

    def assert_get_mp_for_master(self, pid):
        url = env.MASTER + "/metaPartition/get?id=" + str(pid)
        print_url(url)
        result = requests.get(url)

        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)

        data = content["data"]
        print(data)

        assert "VolName" in data
        assert "Zones" in data
        assert "PartitionID" in data
        assert "Status" in data
        assert "DentryCount" in data
        assert "Start" in data
        assert "End" in data
        assert "Hosts" in data
        assert "InodeCount" in data
        assert "IsRecover" in data
        assert "LoadResponse" in data
        assert "MaxInodeID" in data
        assert "MissNodes" in data
        assert "Peers" in data
        assert "ReplicaNum" in data
        assert "Replicas" in data

    def test_read_mp_for_master(self):
        result = self.get_all_mp();
        for pid in result:
            self.assert_get_mp_for_master(pid)
            self.assert_metapartition_load(pid)


    def test_create_mp_for_master(self):
        result = self.get_all_mp();
        for pid in result:
            url = env.MASTER + "/metaPartition/get?id=" + str(pid)
            print_url(url)
            result = requests.get(url)

            assert result.status_code == 200
            content = json.loads(result.content.decode())
            self.assert_base_resp(content)

            data = content["data"]
            print(data)

            start = data["Start"]
            end = data["End"]
            cut = (end - start) / 2
            print(start)
            print(end)
            print(int(cut))

            url = env.MASTER + "/metaPartition/create?name=" + data["VolName"] + "&start=" + str(int(cut))
            print_url(url)
            result = requests.get(url)

            assert result.status_code == 200
            content = json.loads(result.content.decode())
            self.assert_base_resp(content)

            data = content["data"]
            print(data)
            break

    def test_decommission_mp_for_master(self):
        result = self.get_all_mp();
        for pid in result:
            addr = result[pid]

            url = env.MASTER + "/metaPartition/decommission?id=" + str(pid) + "&addr=" + addr
            print_url(url)

            result = requests.get(url)

            assert result.status_code == 200
            content = json.loads(result.content.decode())
            #self.assert_base_resp(content)

            data = content["data"]
            print(data)
            break

    def test_get_client_mp(self):
        rst = get_user_vol_list()
        assert len(rst) > 0

        vol = ""
        for v in rst:
            vol = v
            break

        url = env.MASTER + "/client/metaPartitions?name=" + vol;
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        #print(content)

        for mp in content["data"]:
            #print(mp)
            assert "PartitionID" in mp
            assert "Status" in mp
            assert "DentryCount" in mp
            assert "Start" in mp
            assert "End" in mp
            assert "InodeCount" in mp
            assert "IsRecover" in mp
            assert "MaxInodeID" in mp
            assert "LeaderAddr" in mp
            assert "Members" in mp


if __name__ == '__main__':
    unittest2.main(verbosity=2)

if __name__ == "__main__":
    pytest.main(["-s", "test_metapartition.py"])
