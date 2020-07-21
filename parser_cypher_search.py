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


    '''
    执行属性、比较符号的匹配，返回两个个二维列表
    例如：
    [['长度', '大于', '20', '小于', '40'], ['高度', '高于', '10']]
    [['n_attri', 'n_compares', 'n_number', 'n_compares', 'n_number'], ['n_attri', 'n_compares', 'n_number']]
    '''

    def match_attri_comp(self, final_dict):
        pattern = list(final_dict.values())
        words = list(final_dict.keys())
        token = [[0] * len(pattern)][0]  # 定义全零列表作为flag
        listsPattern = []  # 存储n_compares这样的标记
        listsValue = []  # 存储对应的值
        lists = []
        for i in range(len(pattern)):
            if (pattern[i] == 'n_attri'):
                list1 = []
                list2 = []
                list1.append(words[i])
                list2.append(pattern[i])
                for j in range(i):  # 先匹配前面的
                    if (pattern[j] == 'n_compares' and token[j] == 0):
                        list1.append(words[j])
                        list2.append(pattern[j])
                        token[j] = 1
                    if (pattern[j] == 'n_number' and token[j] == 0):
                        list1.append(words[j])
                        list2.append(pattern[j])
                        token[j] = 1
                if (len(list1) == 1):  # 前面没匹配到，匹配后面的
                    for j in range(i + 1, len(pattern)):
                        if (pattern[j] == 'n_attri'):
                            break
                        if (pattern[j] == 'n_compares' and token[j] == 0):
                            list1.append(words[j])
                            list2.append(pattern[j])
                            token[j] = 1
                        if (pattern[j] == 'n_number' and token[j] == 0):
                            list1.append(words[j])
                            list2.append(pattern[j])
                            token[j] = 1
                listsValue.append(list1)
                listsPattern.append(list2)
        return listsValue, listsPattern


    '''针对不同的问题，分开进行处理 成为 Cypher'''
    def parse2cypher2answer(self, final_dict):
        pattern = list(final_dict.values())
        words = list(final_dict.keys())
        pattern_num_dict = defaultdict(int)
        for p in pattern:
            pattern_num_dict[p] += 1
        # print(default_dict)
        # print(pattern)
        # 单实体单属性查询  E-2C Hawkeye Group I的长
        list1 = ['n_aircraft_name', 'n_attri']
        list2 = ['n_big_cates', 'n_attri', 'n_mosts']
        list3 = ['n_attri', 'n_compares', 'n_number']
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

        # 多实体单属性查询  E-2C Hawkeye Group I和Tu-142MK Bear F Mod 3以及F/A-18A Hornet的长是多少？
        elif(set(pattern) == set(list1) and pattern_num_dict['n_attri'] == 1 and pattern_num_dict['n_aircraft_name'] >= 1):
            entitys = [''.join(k) for k, v in final_dict.items() if v == 'n_aircraft_name']

            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            fields = [self.parser.attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, fields[0]) for entity in entitys]
            print(sql)
            answers = []
            for sql_ in sql:
                ress = self.g.run(sql_).data()
                answers.append(ress[0]['m.{0}'.format(fields[0])])

            answer = "      ".join(answers)

        # 多实体多属性查询  E-2C Hawkeye Group I和Tu-142MK Bear F Mod 3以及F/A-18A Hornet的长宽高是多少？
        elif (set(pattern) == set(list1) and pattern_num_dict['n_attri'] >= 1 and pattern_num_dict[
            'n_aircraft_name'] >= 1):
            entitys = [''.join(k) for k, v in final_dict.items() if v == 'n_aircraft_name']
            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            fields = [self.parser.attri2fields.get(i) for i in attri]
            sql = ["MATCH (m:DataAircraft) where m.Name = '{0}' return m.{1} LIMIT 1".format(entity, field) for entity
                   in entitys for field in fields]
            answers = []
            ans = []
            for i, sql_ in enumerate(sql):
                ress = self.g.run(sql_).data()
                ans.append(ress[0]['m.{0}'.format(fields[i % len(fields)])])
                if i % len(fields) == len(fields) - 1:
                    answers.append("      ".join(ans))
                    ans = []
            answer = '\n'.join(answers)


        # 单属性单区间问答 # 长度大于20的轰炸机 and 单属性多区间问答  # 长度大于20 小于40的轰炸机
        elif (set(list3).issubset(set(pattern)) and pattern_num_dict['n_attri']==1 and pattern_num_dict['n_compares']>=1):
            # print('单属性多区间问答')
            entity = "".join([k for k, v in final_dict.items() if v == 'n_big_cates'])
            entity_field = self.parser.big_cates_dict_1to1.get(entity)
            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            attri_field = "".join([self.parser.attri2fields.get(i) for i in attri])  # 单属性可以这么用
            attri_field = "toFloat(m." + attri_field +")"  # ##注意此处的m代表的是下面sql的实体 表示，只是为了拼接方便
            # print(pattern)
            com_nums = []
            i = 0
            while i < len(pattern) - 1:
                if (set([pattern[i] + pattern[i + 1]]).issubset(set(['n_comparesn_number', 'n_numbern_compares']))):
                    if( pattern[i] == 'n_compares'):
                        comp = self.parser.compares_dict_1to1.get(words[i])
                        com_num = attri_field+comp+words[i+1]
                        com_nums.append(com_num)
                    else:
                        comp = self.parser.compares_dict_1to1.get(words[i+1])
                        com_num = attri_field+comp+words[i]
                        com_nums.append(com_num)
                    i = i + 1
                i = i + 1
            where_content = ''
            for i in com_nums:
                where_content += i
                if(i != com_nums[-1]):
                    where_content += ' '+'and'+' '
            sql = "match (m:{0}) where ({1}) return m.Name".format(entity_field, where_content)
            print(sql)
            ress = self.g.run(sql).data()
            answer = ""
            for i in ress:
                answer += "".join(i.values())
                answer += "     "

        # 多属性多区间问答 # 长度大于40 小于88,并且高度高于10米的轰炸机
        elif (set(list3).issubset(set(pattern)) and pattern_num_dict['n_attri']>1 and pattern_num_dict['n_compares']>1):
            entity = "".join([k for k, v in final_dict.items() if v == 'n_big_cates'])
            entity_field = self.parser.big_cates_dict_1to1.get(entity)
            # print(pattern)
            listsValue, listsPattern = self.match_attri_comp(final_dict)
            com_nums = []
            for k in range(len(listsPattern)):
                flag = [[0] * len(listsPattern[k])][0]  # 定义全零列表作为flag
                for i in range(len(listsPattern[k])):
                    if(listsPattern[k][i] == 'n_attri'):
                        flag[i] = 1
                        attri_1 = "".join([self.parser.attri2fields.get(listsValue[k][i])])
                        attri_1 = "toFloat(m." + attri_1 + ")"  ##注意此处的m代表的是下面sql的实体 表示，只是为了拼接方便
                    elif(listsPattern[k][i] == 'n_compares'):
                        if(listsPattern[k][i-1] == 'n_number' and flag[i-1] == 0):
                            flag[i-1] = 1
                            cmp_num = attri_1 + self.parser.compares_dict_1to1.get(listsValue[k][i]) + listsValue[k][i-1]
                            com_nums.append(cmp_num)
                        elif(listsPattern[k][i+1] == 'n_number' and flag[i+1] == 0):
                            flag[i+1] = 1
                            cmp_num = attri_1 + self.parser.compares_dict_1to1.get(listsValue[k][i]) + listsValue[k][i+1]
                            com_nums.append(cmp_num)

            where_content = ''
            for i in com_nums:
                where_content += i
                if (i != com_nums[-1]):
                    where_content += ' ' + 'and' + ' '
            sql = "match (m:{0}) where ({1}) return m.Name".format(entity_field, where_content)
            print(sql)
            ress = self.g.run(sql).data()
            answer = ""
            for i in ress:
                answer += "".join(i.values())
                answer += "     "


        # 单实体 属性 最值问答
        elif (set(pattern) == set(list2) and len(pattern) == len(list2)): # 战斗机中长度最长是哪个
            entity = "".join([k for k, v in final_dict.items() if v == 'n_big_cates'])
            # print(entity)
            entity_field = self.parser.big_cates_dict_1to1.get(entity)
            # print(self.parser.big_cates_dict_1to1)
            attri = [k for k, v in final_dict.items() if v == 'n_attri']
            attri_field = "".join([self.parser.attri2fields.get(i) for i in attri])  # 单属性可以这么用
            most = "".join([k for k, v in final_dict.items() if v == 'n_mosts'])

            if (self.parser.mosts_dict_1to1.get(most) == 1):  # 属性最大值
                sql = "match (m:{0}) return m.Name, m.{1} order by toFloat(m.{1}) desc limit 1".format(entity_field,
                                                                                                       attri_field)
            elif (self.parser.mosts_dict_1to1.get(most) == -1):  # 属性最小值
                sql = "match (m:{0}) return m.Name, m.{1} order by toFloat(m.{1}) asc limit 1".format(entity_field,
                                                                                                       attri_field)
            print(sql)
            answer = ""
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