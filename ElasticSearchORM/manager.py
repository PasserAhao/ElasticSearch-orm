# coding=utf-8
from elasticsearch import Elasticsearch
from ElasticSearchORM.query import Queryset


class MElasticSearch:
    def __init__(self, *args, **kwargs):
        self.es_model = Elasticsearch(*args, **kwargs)
        self.es_model = None

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
    pass
