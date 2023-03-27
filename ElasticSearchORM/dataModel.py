# coding=utf-8
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
        values = self.search_data
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


