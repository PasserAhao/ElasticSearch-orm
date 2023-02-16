# coding=utf-8
from elasticsearch import Elasticsearch

"""



"""


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


class Queryset:
    def __init__(self, index_list, es_obj, size=100):
        self.es = es_obj
        self._index = index_list
        self._body = {
            "size": str(size),
            "query": {
            },
        }

    def values(self, *args):
        """
        选择需要保留的字段
        :param args:
        :return:
        """
        if self._body.has_key("_source"):
            self._body["_source"] += map(str, args)
        self._body["_source"] = map(str, args)

        return self

    def order_by(self, *args):
        """
        选择需要排序的字段，字段前端加上-表示为倒叙  例如： -age 按照年龄倒叙
        该方法支持传多个参数，排序会优先按照第一个字段排序，如果相同，则会根据第二个字段排序
        :param args:
        :return:
        """
        if any(map(lambda y: not (isinstance(y, str) and bool(y)), args)):
            raise TypeError("order_by 方法每个参数必须都是非空字符串")

        if self._body.has_key("sort"):
            self._body["sort"] += [
                {field[1:]: {"order": "desc"}} if field[0] == "-" else {field: {"order": "asc"}} for field in args
            ]
            return self
        self._body["sort"] = [
            {field[1:]: {"order": "desc"}} if field[0] == "-" else {field: {"order": "asc"}} for field in args
        ]
        return self

    def group_by(self, *args):
        """
        选择根据字段分组
        :param args:
        :return:
        """
        if not self._body.has_key("aggs"):
            self._body["aggs"] = {}
        _aggs = self._body["aggs"]
        for item in args:
            if isinstance(item, str):
                _aggs[(str(item) + "_value")] = {
                    "terms": {"field": str(item)}
                }
            # todo 处理更加复杂的聚合
            if isinstance(item, tuple):
                pass
        return self

    def limit(self, size):
        """
        选择需要展示数据的条数
        :param size:
        :return:
        """
        if isinstance(size, str):
            if not size.isdigit():
                raise TypeError("size 必须是int类型，或者纯数字文本")
            self._body["size"] = int(size)
            return self

        if not isinstance(size, int):
            raise TypeError("size 必须是int类型，或者纯数字文本")
        self._body["seize"] = size
        return self

    def filter(self, search_type="must", **kwargs):
        """
        eg: id=1,title__lt=20,
        age__range=(10,20)
        __endswith="asd",__icontains="asd",
        __regex=f"^2"
        """
        if not self._body["query"].has_key("bool"):
            self._body["query"]["bool"] = {}
        if not self._body["query"]["bool"].has_key(search_type):
            self._body["query"]["bool"][search_type] = []
        type_list = self._body["query"]["bool"][search_type]
        for key, value in kwargs.items():
            self.__create_filter(key, value, type_list)
        return self

    def all(self):
        # 对返回的数据进行一次封装
        res_data = self.es.search(index=self._index, body=self._body)
        if isinstance(res_data, dict):
            return DictModel(res_data)
        return res_data

    def __create_filter(self, key, value, lis):
        if "__" not in key:
            if key != "query_string":
                con_list = self.__find_item("terms", lis)
                if key not in con_list:
                    con_list[key] = [str(value)]
                else:
                    con_list[key].append(str(value))
                return

            con_list = self.__find_item("query_string", lis)
            con_list["query"] = str(value)

        else:
            field, condition = key.split("__")
            if not field or not condition:
                raise TypeError("filter 错误的条件格式  __ 前后不能为空")

            dic = {
                "gte": self.__range,
                "lte": self.__range,
                "gt": self.__range,
                "lt": self.__range,
                "icontains": self.__icontaions,
                "contains": self.__contains,
                "startswith": self.__startswith,
                "endswith": self.__endswith,
                "regexp": self.__regexp,
                "null": lambda x, y, z, q: None,
            }
            dic.get(condition, "null")(field, condition, value, lis)

    def __find_item(self, key, lis):
        for item in lis:
            if key in item:
                return item[key]
        lis.append({key: {}})
        return self.__find_item(key, lis)

    def __range(self, field, condition, value, lis):
        con_list = self.__find_item("range", lis)
        if field not in con_list:
            con_list[field] = {condition: value}
        con_list[field][condition] = value

    def __icontaions(self, field, condition, value, lis):
        con_list = self.__find_item("match", lis)
        con_list[field] = value

    def __contains(self, field, condition, value, lis):
        con_list = self.__find_item("match", lis)
        con_list[field] = {
            "query": value,
            "case_sensitive": "true"
        }

    def __startswith(self, field, condition, value, lis):
        con_list = self.__find_item("wildcard")
        con_list[field] = str(value) + "*"

    def __endswith(self, field, condition, value, lis):
        con_list = self.__find_item("wildcard")
        con_list[field] = "*" + str(value)

    def __regexp(self, field, condition, value, lis):
        con_list = self.__find_item("regexp", lis)
        con_list[field] = value


class MElasticSearch:
    def __init__(self, *args, **kwargs):
        self.es_model = Elasticsearch(*args, **kwargs)

    def object(self, index_list):
        try:
            return Queryset(index_list, self.es_model)
        except Exception as e:
            raise e

    @property
    def es(self):
        """
        返回es对象，方便使用原生的es
        :return:
        """
        return self.es

    def get_alias(self, *args, **kwargs):
        return self.es_model.indices.get_alias(*args, **kwargs)


if __name__ == "__main__":
    query = Queryset("")
    # query.values("event", "user_id", "biz_name", "event_value1", "biz")
    # query.filter(query_string="message:event_log AND biz:345 AND (event:2 OR event:1)", user_id="3106190")
    query.group_by("event_value")

    print "\n\n\n\n", query._body
