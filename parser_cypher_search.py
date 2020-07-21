# coding: utf-8
# Author: rjq<ruanjingqing@126.com>
# Date: 2020-07-18

from py2neo import Graph
from question_parser import *
from collections import defaultdict

'''
用户：E-2C Hawkeye Group I的长度是什么
{'n_aircraft_name': ['E-2C Hawkeye Group I'], 'n_attri': ['长度']}
'''


class AnswerSearcher:
    def __init__(self):
        self.g = Graph(
            # "http://localhost:7474/db/data"  # py2neo 2.0.8写法
            host="127.0.0.1",  # py2neo 3写法
            user="neo4j",
            password="Rjq519623"
        )
        self.num_limit = 30
        self.parser = QuestionParser() # 为了引用这个类中的变量

    '''执行cypher查询，并返回相应结果'''
    def search_main(self, dict):
        final_answers = []
        if(dict['pattern']=='entity_attri'):# 单实体单属性查询
            for sql_ in dict['sql']:
                # question_type = sql_['question_type']
                # queries = sql_['sql']
                ress = self.g.run(sql_).data()
                # print(ress)
                final_answers.append(ress[0])
            # 解析成最终答案
        return final_answers

    '''针对不同的问题，分开进行处理 成为 Cypher'''
    def parse2cypher2answer(self, final_dict):
        pattern = list(final_dict.values())
        pattern_num_dict = defaultdict(int)
        for p in pattern:
            pattern_num_dict[p] += 1
        # print(default_dict)
        # print(pattern)
        # 单实体单属性查询  E-2C Hawkeye Group I的长
        list1 = ['n_aircraft_name', 'n_attri']
        list2 = ['n_big_cates', 'n_attri','n_mosts']
        if(set(pattern)==set(list1) and pattern_num_dict['n_attri']==1 and pattern_num_dict['n_aircraft_name']==1):
            entity = "".join([k for k, v in final_dict.items() if v == 'n_aircraft_name'])
            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            field = "".join([self.parser.attri2fields.get(i) for i in attri])  # 单属性可以这么用
            # print(field)
            sql = "MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, field)
            print(sql)
            ress = self.g.run(sql).data()
            answer = ress[0]['m.{0}'.format(field)]

        # 单实体多属性查询  E-2C Hawkeye Group I的长宽高
        elif(set(pattern)==set(list1) and pattern_num_dict['n_attri']>=1 and pattern_num_dict['n_aircraft_name']==1):
            entity = "".join([k for k, v in final_dict.items() if v == 'n_aircraft_name'])
            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            fields = [self.parser.attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, i) for i in fields ]
            print(sql)
            dict_answer = {}
            for sql_ in sql:
                ress = self.g.run(sql_).data()
                dict_answer.update(ress[0])
            answer = ""
            for i in fields:
                answer += dict_answer['m.{0}'.format(i)]
                answer += "     "

        # 属性多区间问答  # 长度大于20小于40的轰炸机



        # 单实体 属性 最值问答
        elif (set(pattern) == set(list2) and len(pattern) == len(list2)): # 战斗机中长度最长是哪个
            entity = "".join([k for k, v in final_dict.items() if v == 'n_big_cates'])
            # print(entity)
            entity_field = self.parser.big_cates_dict_1to1.get(entity)
            print(self.parser.big_cates_dict_1to1)
            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            attri_field = "".join([self.parser.attri2fields.get(i) for i in attri])  # 单属性可以这么用
            most = "".join([k for k, v in final_dict.items() if v == 'n_mosts'])

            answer = ""
            if (self.parser.mosts_dict_1to1.get(most) == 1):  # 属性最大值
                sql = "match (m:{0}) return m.Name, m.{1} order by toFloat(m.{1}) desc limit 1".format(entity_field, attri_field)
                print(sql)
            elif (self.parser.mosts_dict_1to1.get(most) == -1):  # 属性最小值
                sql = "match (m:{0}) return m.Name, m.{1} order by toFloat(m.{1}) asc limit 1".format(entity_field,
                                                                                                       attri_field)
                print(sql)

            ress = self.g.run(sql).data()
            for i in ress[0].values():
                answer += i
                answer += "     "

        return answer











if __name__ == '__main__':
    handler = AnswerSearcher()
    final_dict = {'n_aircraft_name': ['E-2C Hawkeye Group I'], 'n_aircraft_attri': ['长度']}
    sqls = parse2cypher(final_dict)
    print(sqls)