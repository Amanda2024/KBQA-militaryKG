# coding: utf-8
# Author: rjq<ruanjingqing@126.com>
# Date: 2020-07-18

from py2neo import Graph
from question_parser import *
from collections import defaultdict

'''
用户：E-2C Hawkeye Group I的长度是什么
{'n_aircraft_name': ['E-2C Hawkeye Group I'], 'n_aircraft_attri': ['长度']}
'''


class AnswerSearcher:
    def __init__(self):
        self.g = Graph(
            # "http://localhost:7474/db/data"  # py2neo 2.0.8写法
            host="127.0.0.1",  # py2neo 3写法
            user="neo4j",
            #password="Rjq519623"
            password="1315882755"
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
        list1 = ['n_aircraft_name', 'n_aircraft_attri']
        list2 = ['n_aircraft_attri']
        if(set(pattern)==set(list1) and pattern_num_dict['n_aircraft_attri']==1 and pattern_num_dict['n_aircraft_name']==1):
            entity = "".join([k for k, v in final_dict.items() if v == 'n_aircraft_name'])
            attri = [k for k, v in final_dict.items() if v == 'n_aircraft_attri']
            field = "".join([self.parser.aircraft_attri2fields.get(i) for i in attri])  # 单属性可以这么用
            # print(field)
            sql = "MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, field)
            print(sql)
            ress = self.g.run(sql).data()
            answer = ress[0]['m.{0}'.format(field)]

        # 单实体多属性查询  E-2C Hawkeye Group I的长宽高
        elif(set(pattern)==set(list1) and pattern_num_dict['n_aircraft_attri']>=1 and pattern_num_dict['n_aircraft_name']==1):
            entity = "".join([k for k, v in final_dict.items() if v == 'n_aircraft_name'])
            attri = [k for k, v in final_dict.items() if v == 'n_aircraft_attri']
            fields = [self.parser.aircraft_attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, i) for i in fields ]
            print(sql)
            dict_answer = {}
            for sql_ in sql:
                ress = self.g.run(sql_).data()
                dict_answer.update(ress[0])
            answer = ""
            for i in fields:
                answer += dict_answer['m.{0}'.format(i)]
                answer += "      "

        # 多实体单属性查询  E-2C Hawkeye Group I和Tu-142MK Bear F Mod 3以及F/A-18A Hornet的长是多少？
        elif (set(pattern) == set(list1) and pattern_num_dict['n_aircraft_attri'] == 1 and pattern_num_dict[
            'n_aircraft_name'] >= 1):
            entitys = [''.join(k) for k, v in final_dict.items() if v == 'n_aircraft_name']
            print(entitys)
            attri = [k for k, v in final_dict.items() if v == 'n_aircraft_attri']
            fields = [self.parser.aircraft_attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, fields[0]) for entity in entitys]
            print(sql)
            answers = []
            for sql_ in sql:
                ress = self.g.run(sql_).data()
                answers.append(ress[0]['m.{0}'.format(fields[0])])

            answer = "      ".join(answers)

        #单属性区间问答  查找爬升率大于1500的飞机有哪些？
        elif (set(pattern) == set(list2) and pattern_num_dict['n_aircraft_attri'] == 1):
            attri =[k for k, v in final_dict.items() if v == 'n_aircraft_attri']
            fields = [self.parser.aircraft_attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where (toFloat(m.{0})>1500) return m.Name".format(fields[0])]
            print(sql)
            answers = []
            for sql_ in sql:
                ress = self.g.run(sql_).data()

            answers = [res['m.Name'] for res in ress]
            answer = "      ".join(answers)

        #多实体多属性查询  E-2C Hawkeye Group I和Tu-142MK Bear F Mod 3以及F/A-18A Hornet的长宽高是多少？
        elif (set(pattern) == set(list1) and pattern_num_dict['n_aircraft_attri'] >= 1 and pattern_num_dict[
            'n_aircraft_name'] >= 1):
            entitys = [''.join(k) for k, v in final_dict.items() if v == 'n_aircraft_name']
            print(entitys)
            attri = [k for k, v in final_dict.items() if v == 'n_aircraft_attri']
            fields = [self.parser.aircraft_attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, field) for entity in entitys for field in fields]
            print(sql)
            answers = []
            ans = []
            for i,sql_ in enumerate(sql):
                ress = self.g.run(sql_).data()
                ans.append(ress[0]['m.{0}'.format(fields[i%len(fields)])])
                if i%len(fields)==len(fields)-1:
                    answers.append("      ".join(ans))
                    ans = []
            answer='\n'.join(answers)

        return answer











if __name__ == '__main__':
    handler = AnswerSearcher()
    final_dict = {'n_aircraft_name': ['E-2C Hawkeye Group I'], 'n_aircraft_attri': ['长度']}
    sqls = parse2cypher(final_dict)
    print(sqls)
