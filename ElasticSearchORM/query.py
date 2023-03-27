# coding=utf-8
from ElasticSearchORM.dataModel import DictModel

class Queryset(object):
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
        if self._body.__contains__("_source"):
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

        if self._body.__contains__("sort"):
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
        if not self._body.__contains__("aggs"):
            self._body["aggs"] = {}
        _aggs = self._body["aggs"]
        for item in args:
            self.__create_group(item, _aggs)

        return self

    def __create_group(self, item, _aggs):
        if isinstance(item, str):
            _aggs[(str(item))] = {
                "terms": {"field": str(item)}
            }
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, str):
                    if value == "distinct":
                        _aggs["distinct_{}".format(key)] = {
                            "cardinality": {"field": key}
                        }
                    if value in ["sum", "avg", "max", "min"]:
                        _aggs["{}_{}".format(value, key)] = {
                            value: {"field": key}
                        }
                if isinstance(value, dict):
                    field_name = "combine_{}".format(key)
                    _aggs[field_name] = {
                        "terms": {"field": key},
                        "aggs": {}
                    }

                    aggs = _aggs[field_name]["aggs"]
                    self.__create_group(value, aggs)

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
        self._body["size"] = size
        return self

    def offset(self, size):
        """
        选择需要展示数据的条数
        :param size:
        :return:
        """
        if isinstance(size, str):
            if not size.isdigit():
                raise TypeError("size 必须是int类型，或者纯数字文本")
            self._body["from"] = int(size)
            return self

        if not isinstance(size, int):
            raise TypeError("size 必须是int类型，或者纯数字文本")
        self._body["from"] = size
        return self

    def filter(self, search_type="must", is_recommend=True, **kwargs):
        """
        eg: id=1,title__lt=20,
        age__range=(10,20)
        __endswith="asd",__icontains="asd",
        __regex=f"^2"
        """
        if not self._body["query"].__contains__("bool"):
            self._body["query"]["bool"] = {}
        if not self._body["query"]["bool"].__contains__(search_type):
            if search_type != "filter":
                self._body["query"]["bool"]["filter"] = []
            self._body["query"]["bool"][search_type] = []

        type_list = self._body["query"]["bool"][search_type]
        filter_list = self._body["query"]["bool"]["filter"]
        for key, value in kwargs.items():
            self.__create_filter(key, value, type_list, filter_list, is_recommend)
        return self

    def __create_filter(self, key, value, type_list, _filter, is_recommend=True):
        if "__" not in key:
            if key != "query_string":
                con_list = self.__find_item("terms", type_list)
                if key not in con_list:
                    con_list[key] = [str(value)]
                else:
                    con_list[key].append(str(value))
                return

            con_list = self.__find_item("query_string", type_list)
            con_list["query"] = str(value)

        else:
            field, condition = key.split("__")
            if not field or not condition:
                raise TypeError("filter 错误的条件格式  __ 前后不能为空")

            dic = {
                "gte": (self.__range, _filter),
                "lte": (self.__range, _filter),
                "gt": (self.__range, _filter),
                "lt": (self.__range, _filter),
                "icontains": (self.__icontaions, type_list),
                "contains": (self.__contains, type_list),
                "startswith": (self.__startswith, type_list),
                "endswith": (self.__endswith, type_list),
                "regexp": (self.__regexp, type_list),
            }
            func_info = dic.get(condition, False)
            if not func_info:
                raise ValueError("没有该类型哦:{}".format(condition))
            filter_func, recommend_list = func_info
            filter_func(field, condition, value, recommend_list if is_recommend else type_list)

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

    def all(self):
        # 对返回的数据进行一次封装
        res_data = self.es.search(index=self._index, body=self._body)
        if isinstance(res_data, dict):
            return DictModel(res_data)
        return res_data