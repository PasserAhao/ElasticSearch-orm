# coding=utf-8
import json
import sys
import time

a = {"hits": {"hits": [{"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "87843c01-ad07-11ed-8f4f-525400bb86ae"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "8789ba41-ad07-11ed-8f4f-525400bb86ae"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "6bc40c25-ad07-11ed-8f4f-525400bb86ae"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "9e19f131-ad07-11ed-8f4f-525400bb86ae"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "93ba7482-ad07-11ed-bf78-5254006e151d"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "9e1695d0-ad07-11ed-bf78-5254006e151d"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "bbbc2783-ad07-11ed-a4e6-525400c4015d"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "93db1be1-ad07-11ed-bf78-5254006e151d"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "6bb7fe31-ad07-11ed-a16b-525400de3512"},
                       {"sort": [292101815], "_type": "message", "_source": {"user_id": 292101815}, "_score": 'null',
                        "_index": "event_1487", "_id": "bbbcead1-ad07-11ed-bf78-5254006e151d"},
                       {"sort": [292088192], "_type": "message", "_source": {"user_id": 292088192}, "_score": 'null',
                        "_index": "event_1486", "_id": "b99aac21-ac0c-11ed-bf78-5254006e151d"},
                       {"sort": [292088192], "_type": "message", "_source": {"user_id": 292088192}, "_score": 'null',
                        "_index": "event_1486", "_id": "b99db961-ac0c-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "e367a640-ac2d-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "e3666dc3-ac2d-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "031b73f3-ac2d-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "9d70ea81-ad08-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "39cf5bf0-ac2d-11ed-a4e6-525400c4015d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "eadbc2d6-ac2d-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "febf93e1-ac2c-11ed-a4e6-525400c4015d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "ef5fb9c0-ac2c-11ed-a4e6-525400c4015d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "469e03e1-ac2d-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "d640fbc1-ad08-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "cf679890-ad08-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "5faefac2-ad08-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "39cee6c1-ac2d-11ed-a16b-525400de3512"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "469f3c61-ac2d-11ed-a16b-525400de3512"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "02ff6071-ac2d-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "febef7a7-ac2c-11ed-a16b-525400de3512"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "ef7898f1-ac2c-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1486", "_id": "eadaff80-ac2d-11ed-a16b-525400de3512"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "45794932-ad0d-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "45947251-ad0d-11ed-a16b-525400de3512"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "cf6834d1-ad08-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "d6405f80-ad08-11ed-a4e6-525400c4015d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "5fa25090-ad08-11ed-bf78-5254006e151d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "9d89f0c1-ad08-11ed-8f4f-525400bb86ae"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "bdf35d62-ad08-11ed-a4e6-525400c4015d"},
                       {"sort": [292087550], "_type": "message", "_source": {"user_id": 292087550}, "_score": 'null',
                        "_index": "event_1487", "_id": "bdf53221-ad08-11ed-a16b-525400de3512"},
                       {"sort": [291692907], "_type": "message", "_source": {"user_id": 291692907}, "_score": 'null',
                        "_index": "event_1486", "_id": "5f3b0354-ac44-11ed-8f4f-525400bb86ae"},
                       {"sort": [291692907], "_type": "message", "_source": {"user_id": 291692907}, "_score": 'null',
                        "_index": "event_1486", "_id": "5f3a6713-ac44-11ed-bf78-5254006e151d"},
                       {"sort": [291631472], "_type": "message", "_source": {"user_id": 291631472}, "_score": 'null',
                        "_index": "event_1486", "_id": "ec9d3e62-ac2c-11ed-bf78-5254006e151d"},
                       {"sort": [291631472], "_type": "message", "_source": {"user_id": 291631472}, "_score": 'null',
                        "_index": "event_1486", "_id": "ecbf1e41-ac2c-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1486", "_id": "0c7eb751-ac22-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1486", "_id": "e040ba30-ac21-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "0bf68ff0-acf0-11ed-a4e6-525400c4015d"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "0f842380-acf0-11ed-a4e6-525400c4015d"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "0f60e411-acf0-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "4a3dbfd0-acf1-11ed-8f4f-525400bb86ae"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "4a5629d1-acf1-11ed-8f4f-525400bb86ae"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "8f6cf8e1-acf2-11ed-a4e6-525400c4015d"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1486", "_id": "d8680f70-ac21-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1486", "_id": "d88b27d0-ac21-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1486", "_id": "e020fd31-ac21-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "0bcdab31-acf0-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "9a3f7e83-acf9-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "8f5aa961-acf2-11ed-8f4f-525400bb86ae"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1486", "_id": "0c71bf01-ac22-11ed-8f4f-525400bb86ae"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "9a3f7e81-acf9-11ed-a16b-525400de3512"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "918096a1-acf2-11ed-bf78-5254006e151d"},
                       {"sort": [291487747], "_type": "message", "_source": {"user_id": 291487747}, "_score": 'null',
                        "_index": "event_1487", "_id": "91885ed1-acf2-11ed-bf78-5254006e151d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1486", "_id": "2615e9b0-ac43-11ed-a16b-525400de3512"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1486", "_id": "290122c1-ac43-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "0481f6d1-acf3-11ed-bf78-5254006e151d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "0f780e31-acf3-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "cfc15de1-acf3-11ed-bf78-5254006e151d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "aeb42871-acf9-11ed-a4e6-525400c4015d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "b45fbe30-acf2-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1486", "_id": "699f8181-ac22-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "cebb8171-ad05-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "aeb3b340-acf9-11ed-bf78-5254006e151d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "a079bfb3-acf2-11ed-a16b-525400de3512"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "a0a2a471-acf2-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "b45fe541-acf2-11ed-a4e6-525400c4015d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "fc8b63d0-acf2-11ed-bf78-5254006e151d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "0f783540-acf3-11ed-a4e6-525400c4015d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1486", "_id": "263dbd01-ac43-11ed-a16b-525400de3512"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1486", "_id": "290122c0-ac43-11ed-a16b-525400de3512"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1486", "_id": "6999dc32-ac22-11ed-a16b-525400de3512"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "fc8f8281-acf2-11ed-a4e6-525400c4015d"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "0481f6d0-acf3-11ed-8f4f-525400bb86ae"},
                       {"sort": [288415363], "_type": "message", "_source": {"user_id": 288415363}, "_score": 'null',
                        "_index": "event_1487", "_id": "ceb4cab2-ad05-11ed-8f4f-525400bb86ae"},
                       {"sort": [288143575], "_type": "message", "_source": {"user_id": 288143575}, "_score": 'null',
                        "_index": "event_1487", "_id": "be401111-acf8-11ed-8f4f-525400bb86ae"},
                       {"sort": [288143575], "_type": "message", "_source": {"user_id": 288143575}, "_score": 'null',
                        "_index": "event_1487", "_id": "be6a5561-acf8-11ed-8f4f-525400bb86ae"},
                       {"sort": [288143575], "_type": "message", "_source": {"user_id": 288143575}, "_score": 'null',
                        "_index": "event_1487", "_id": "e4710f01-ad35-11ed-8f4f-525400bb86ae"},
                       {"sort": [288143575], "_type": "message", "_source": {"user_id": 288143575}, "_score": 'null',
                        "_index": "event_1487", "_id": "e4907de1-ad35-11ed-bf78-5254006e151d"},
                       {"sort": [286875855], "_type": "message", "_source": {"user_id": 286875855}, "_score": 'null',
                        "_index": "event_1487", "_id": "e6653cb1-ad1b-11ed-bf78-5254006e151d"},
                       {"sort": [286875855], "_type": "message", "_source": {"user_id": 286875855}, "_score": 'null',
                        "_index": "event_1487", "_id": "e665d8f1-ad1b-11ed-8f4f-525400bb86ae"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1487", "_id": "01bb4d31-ad29-11ed-a4e6-525400c4015d"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1487", "_id": "03ebc1c0-ad29-11ed-bf78-5254006e151d"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1486", "_id": "5a56fe22-ac5d-11ed-8f4f-525400bb86ae"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1486", "_id": "747739a2-ac5d-11ed-bf78-5254006e151d"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1486", "_id": "5e1edb93-ac5d-11ed-a16b-525400de3512"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1486", "_id": "7463d8b2-ac5d-11ed-bf78-5254006e151d"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1487", "_id": "01c2a034-ad29-11ed-a4e6-525400c4015d"},
                       {"sort": [286046624], "_type": "message", "_source": {"user_id": 286046624}, "_score": 'null',
                        "_index": "event_1487", "_id": "03cf6020-ad29-11ed-8f4f-525400bb86ae"},
                       {"sort": [284545623], "_type": "message", "_source": {"user_id": 284545623}, "_score": 'null',
                        "_index": "event_1487", "_id": "8e3212a1-ad18-11ed-a16b-525400de3512"},
                       {"sort": [284545623], "_type": "message", "_source": {"user_id": 284545623}, "_score": 'null',
                        "_index": "event_1487", "_id": "ad27a3f0-ad18-11ed-a16b-525400de3512"},
                       {"sort": [284545623], "_type": "message", "_source": {"user_id": 284545623}, "_score": 'null',
                        "_index": "event_1487", "_id": "e792e970-ad2a-11ed-8f4f-525400bb86ae"},
                       {"sort": [284545623], "_type": "message", "_source": {"user_id": 284545623}, "_score": 'null',
                        "_index": "event_1487", "_id": "7afa4191-ad30-11ed-8f4f-525400bb86ae"},
                       {"sort": [284545623], "_type": "message", "_source": {"user_id": 284545623}, "_score": 'null',
                        "_index": "event_1487", "_id": "8e1b2f42-ad18-11ed-8f4f-525400bb86ae"}], "total": 646,
              "max_score": 'null'}, "_shards": {"successful": 9, "failed": 0, "skipped": 0, "total": 9}, "took": 26,
     "aggregations": {"event_value1_value": {
         "buckets": [{"key": 14, "doc_count": 306}, {"key": 12, "doc_count": 231}, {"key": 13, "doc_count": 85},
                     {"key": 7, "doc_count": 12}, {"key": 10, "doc_count": 10}, {"key": 11, "doc_count": 2}],
         "sum_other_doc_count": 0, "doc_count_error_upper_bound": 0}}, "timed_out": False}


class ValueModel:
    def __init__(self, dic):
        self.__data = dic


class DictModel(dict):
    def __init__(self, dic):
        dict.__init__(self, dic)

    def __getattr__(self, key):
        try:
            if isinstance(self[key], dict):
                return DictModel(self[key])
            else:
                return self[key]
        except:
            return None

    def __setattr__(self, key, value):
        self[key] = value

    @property
    def all(self):
        """
        返回原有信息
        :return:
        """
        return self

    @property
    def search_data(self):
        """
        返回符合条件的数据列表
        :return:
        """
        try:
            return self["hits"]["hits"]
        except:
            return []

    @property
    def group_data(self):
        """
        返回聚合数据
        :return:
        """
        try:
            if isinstance(self["aggregations"], dict):
                return DictModel(self["aggregations"])
        except:
            return {}

    def filter(self, **kwargs):
        """
        对数据进行二次过滤
        :param kwargs:
        :return:
        """
        values = self.data
        func = lambda y: self.__filter_func(y.get("_source"), kwargs)
        return filter(func, values)

    def order_by(self, *args):

        pass

    @staticmethod
    def __filter_func(data_, conditions):
        for key, value in conditions.items():
            if "__" not in key:
                if data_.get(key, -1) != value:
                    return False
            else:
                field, condition = key.split("__")
                if not field or not condition:
                    raise TypeError("filter 错误的条件格式  __ 前后不能为空")
                dic = {
                    "lte": lambda y: y.get(field) <= value,
                    "gte": lambda y: y.get(field) >= value,
                    "lt": lambda y: y.get(field) < value,
                    "gt": lambda y: y.get(field) > value,
                    "range": lambda y: value[0] < y.get(field) < value[1] if isinstance(value, tuple) and len(
                        value) >= 2 else False,
                    "startswith": lambda y: str(y.get(field)).startswith(str(value)),
                    "endswith": lambda y: str(y.get(field)).endswith(str(value)),
                    "icontains": lambda y: str(value).lower() in str(y.get(field)).lower(),
                    "contains": lambda y: str(value) in str(y.get(field)),
                    "null": lambda: False,
                }
                try:
                    return dic.get(condition, "null")(data_)
                except:
                    return False
        return True


data = DictModel(a)
# print data
# print data.data
print data.filter(user_id__gt=292101814)
