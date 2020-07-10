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

class UserTestCase(unittest2.TestCase):

    def assert_base_resp(self, content):
        #TODO 没有一个统一的格式
        assert 'code' in content
        #print(content['code'])
        assert(content['code'] == 0 or content['code'] == 200 or content['code'] == 303)
        assert 'msg' in content
        #assert content['msg'] == "ok"
        assert 'data' in content


    def test_user_list(self):
        rst = get_user_vol_list()
        assert len(rst) > 0

        u = ""
        for vol in rst:
            u = rst[vol]
            break

        url = env.MASTER + "/user/list?keywords=" + u;
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        print(content)

        for u in content["data"]:
            assert "Mu" in u
            assert "access_key" in u
            assert "create_time" in u
            assert "policy" in u
            assert "secret_key" in u
            assert "user_id" in u
            assert "user_type" in u

            #for i in u["policy"]:
            #print(i)
            #print(u["policy"])
            #assert "authorized_vols" in u["policy"][i] #TODO what's inside
            #assert "own_vols" in u["policy"][i]

    def atest_user_get(self):
        rst = get_user_vol_list()
        assert len(rst) > 0

        u = ""
        for vol in rst:
            u = rst[vol]
            break

        url = env.MASTER + "/user/info?user=" + u;
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        #print(content)

        assert "Mu" in content["data"]
        assert "access_key" in content["data"]
        assert "create_time" in content["data"]
        assert "policy" in content["data"]
        assert "secret_key" in content["data"]
        assert "user_id" in content["data"]
        assert "user_type" in content["data"]

            #for i in u["policy"]:
            #print(i)
            #print(u["policy"])
            #assert "authorized_vols" in u["policy"][i] #TODO what's inside
            #assert "own_vols" in u["policy"][i]

    def test_user_create_update_delete(self):
        url = env.MASTER + "/user/create"
        print_url(url)
        d = json.dumps({"id":"testuser","pwd":"12345","type":3})
        print(d)
        result = requests.post(url, data = d)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

        url = env.MASTER + "/user/update"
        print_url(url)
        d = json.dumps({"user_id":"testuser","access_key":"KzuIVYCFqvu0b3Rd","secret_key":"iaawlCchJeeuGSnmFW72J2oDqLlSqvA5","type":3})
        print(d)
        result = requests.post(url, data = d)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

        url = env.MASTER + "/user/akInfo?ak=KzuIVYCFqvu0b3Rd"
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

        rst = get_user_vol_list()
        assert len(rst) > 0

        v = ""
        for vol in rst:
            v = rst[vol]
            break

        url = env.MASTER + "/user/updatePolicy"
        print_url(url)
        d = json.dumps({'user_id':'testuser','volume':v,'policy':['perm:builtin:ReadOnly','perm:custom:PutObjectAction']})
        print(d)
        result = requests.post(url, data = d)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

        url = env.MASTER + "/user/removePolicy"
        print_url(url)
        d = json.dumps({'user_id':'testuser','volume':v})
        print(d)
        result = requests.post(url, data = d)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

        url = env.MASTER + "/user/transferVol"
        print_url(url)
        d = json.dumps({"volume":"ltptest","user_src":"testuser","user_dst":"ltptest","force":True})
        print(d)
        result = requests.post(url, data = d)
        #assert result.status_code == 200
        content = json.loads(result.content.decode())
        #self.assert_base_resp(content)
        print(content)

        url = env.MASTER + "/user/delete?user=testuser"
        print_url(url)
        result = requests.get(url)
        assert result.status_code == 200
        content = json.loads(result.content.decode())
        self.assert_base_resp(content)
        print(content)



if __name__ == '__main__':
    unittest2.main(verbosity=2)

if __name__ == "__main__":
    pytest.main(["-s", "test_user.py"])
