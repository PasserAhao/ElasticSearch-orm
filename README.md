# ElasticSearch-orm

## 介绍

根据django的orm风格封装出es的orm，如果你使用过django
那么这个插件一定会让你感到如至家归 
地址：[源码地址](https://github.com/PasserAhao/ElasticSearch-orm)

## 安装教程

- 直接把文件copy到根目录下
- 安装依赖包 pip install elasticsearch

## 快速上手

```python
# 搜索索引为class里边 班级名称有 英雄 年龄大于25岁的所有人的  name和id字段 并且根据age倒叙排序，如果age相同则按score正序排序
from ElasticSearchORM import MElasticSearch

# 1. 创建es链接实例
es = MElasticSearch(['127.0.0.1', ])

# 2.选择索引(返回的是一个queryset对象)
class_ = es.object(["class"])

# 3.选择输入字段
class_.values("id", "name")

# 4.选择过滤条件
class_.filter(class_name__icontains="英雄", age__gt=25)

# 5.选择排序
class_.order_by("-age", "score")

# 6.输出结果（原始数据，是一个ValueModel对象）
result_data = class_.all()

# 输出符合条件的数据列表
print(result_data.search_data)
# 输出聚合结果
print(result_data.group_data)
# 对结果进行二次过滤
print(result_data.filter(age__lt=38))

# 简写
result_data = es.object(["class"]).values("id","name").filter(class_name__icontains="英雄", age__gt=25).order_by("-age","score").all()

```


## API说明

### object（index_list）

index_list为索引名称列表，等价于es.search方法中的index参数
在执行object熟悉后会返回一个Queryset对象

### values（*args）

eg:.values("id","name","score")
这里输入需要输出的字段名称

### filter(**kwargs)
过滤条件选择，过滤出最终符合条件的结果

详细说明：
1. 选择age大于，大于等于，小于，小于等于18的值
> .filter(age__gt=18)
> .filter(age__gte=18)
> .filter(age__lt=18)
> .filter(age__lte=18)
2. 。。。

### order_by(*args)

### group_by(*args)

### limit(size)

### all()


## 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request
