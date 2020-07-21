# coding: utf-8
# Author: rjq<ruanjingqing@126.com>
# Date: 2020-07-17

import os
import jieba
import jieba.posseg as pseg
import re
import ahocorasick

class QuestionParser:
    def __init__(self):
        cur = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        # 领域特征词路径(@modify) 或者 KG字段-特征词的词典
        self.aircraft_name_path = os.path.join(cur, 'data/aircraft_name.txt')
        # self.aircraft_attri_path = os.path.join(cur, 'data/aircraft_attri.txt')
        self.attri_dict = {
            'Agility': [''],
            'AircraftCockpitArmor': [''],
            'AircraftEngineArmor': [''],
            'AircraftFuselageArmor': [''],
            'ClimbRate': ['攀升率'],
            'Comments': [''],
            '-Cost': [''],
            'Crew': [''],
            'DamagePoints': [''],
            'Deprecated': [''],
            'FuelOffloadRate': [''],
            'Hypothetical': [''],
            'ID': [''],
            'Length': ['长度','长','总长', '全长', '多长'],
            'Span': ['宽','宽度','总宽','多宽'],
            'Height': ['高','高度','总高', '多高'],
            'Name': ['名字'],
            'OODADetectionCycle': [''],
            'DAEvasiveCycle': [''],
            'OODATargetingCycle': [''],
            'TotalEndurance': [''],
            'WeightEmpty': [''],
            'WeightMax': [''],
            'WeightPayload': [''],
            'YearCommissionied': [''],
            'YearDecommissioined': ['']
            }
        # KG字段-特征词的词典 转换成 特征词-KG字段词典
        # 即：键对多值 反过来转成==> 键对单值
        self.attri2fields = self.attri_dict2fields_dict(self.attri_dict)
        self.mosts_dict = {
            1: ['最大', '最远', '最长', '最高', '最久', '最快', '最多', '最强', '最宽'],
            -1: ['最小', '最短', '最近', '最低', '最矮', '最慢', '最少', '最弱'],
        }
        self.mosts_dict_1to1 = self.attri_dict2fields_dict(self.mosts_dict)
        self.big_cates_dict = { # 指的是含有某些属性的节点集的大类名称(//需要补充)
            'DataAircraft':['飞机','战斗机','客机','轰炸机'],
            'DataSensor':['传感器','敏感元件','探测设备']
        }
        self.big_cates_dict_1to1 = self.attri_dict2fields_dict(self.big_cates_dict)
        self.compares_dict = {
            '$gt': ['高于', '大于', '长于', '高过', '大过', '长过', '多于', '远于', '远过', '之后', '晚于', '后于'],
            '$lt': ['低于', '小于', '短于', '低过', '短过', '少于', '近于', '近过', '未达到', '没达到', '之前', '先于', '早于'],
            '$lte': ['不高于', '不大于', '不长于', '不高过', '不大过', '不长过', '不多于', '不远于', '不远过'],
            '$gte': ['不低于', '不小于', '不短于', '不低过', '不短过', '不少于', '不近于', '不近过', '达到'],
            '$eq': ['等于', '差不多'],
            '$ne': ['不等于', '不是']}
        self.compares_dict_1to1 = self.attri_dict2fields_dict(self.compares_dict)


        # 加载领域特征词(@modify)
        self.aircraft_name = [i.strip() for i in open(self.aircraft_name_path, encoding='utf-8') if i.strip()] # 去重前4728，去重后2173
        # self.aircraft_attri = [i.strip() for i in open(self.aircraft_attri_path, encoding='utf-8') if i.strip()] #
        self.attri = self.dictvalue2list(self.attri_dict)
        self.mosts = self.dictvalue2list(self.mosts_dict)
        self.big_cates = self.dictvalue2list(self.big_cates_dict)
        self.compares = self.dictvalue2list(self.compares_dict)

        # 集合特征词并且去重(@modify)
        self.region_words = set(self.aircraft_name + self.attri + self.mosts + self.big_cates + self.compares) # self.aircraft + self.XXX


        # 构造领域actree
        self.region_tree = self.build_actree(list(self.region_words))

        # 构建领域词以及对应类别的词典
        self.wdtype_dict = self.build_wdtype_dict()


    '''构造词对应的类型''' #(@modify)
    def build_wdtype_dict(self):
        wd_dict = dict()
        for wd in self.region_words:
            wd_dict[wd] = []
            if wd in self.aircraft_name:
                wd_dict[wd].append('n_aircraft_name')
            if wd in self.attri:
                wd_dict[wd].append('n_attri')
            if wd in self.mosts:
                wd_dict[wd].append('n_mosts')
            if wd in self.big_cates:
                wd_dict[wd].append('n_big_cates')
            if wd in self.compares:
                wd_dict[wd].append('n_compares')
            # if ……
        return wd_dict

    '''提取字典中的值存储为列表'''
    def dictvalue2list(self, dict):
        list = []
        for k in dict.keys():
            if(isinstance(k,int)): # add 07.20
                True
            else:
                list.append(k)
                list.append(k.lower())  # 添加 小写
            for i in dict[k]:
                list.append(i) if (len(i) != 0) else False
        # print(list)
        return list

    '''KG字段-特征词的词典 转换成 特征词-KG字段的词典'''
    def attri_dict2fields_dict(self, dict):
        wd_dict = {}
        for cate, wds in dict.items():
            for wd in wds:
                if(len(wd) != 0):
                    wd_dict[wd] = cate
        return wd_dict

    '''加载实体'''
    # def load_entity(self):
    #     entity = []
    #     with open(self.aircraftpath, 'r', encoding='utf-8') as file:
    #         entity = file.readlines()  # 读取所有行并返回列表
    #         entity = [x.strip() for x in entity]
    #     # print(len(list(set(entity))))
    #
    #     return list(set(entity))

    '''将实体标记和实体词加入到jieba当中'''
    def add_jieba(self, wds, tag):
        for wd in wds:
            jieba.add_word(wd, tag=tag, freq=300000)
        return

    '''英文 短语识别(以下暂时没用到)，使用ahocorasick树即可'''
    def extract_ner(self, question):
        question = re.sub("[?？]+", "问号", question)  # 问号替换掉
        question = re.sub("[\u4E00-\u9FA5]+", "\n", question)  # 中文去掉
        tmp_list = question.splitlines()  # 删除长度小于1的字符串
        en_list = [i for i in tmp_list if len(i) > 1]
        # print(en_list)
        return en_list

    '''构造actree，加速过滤'''
    def build_actree(self, wordlist):
        actree = ahocorasick.Automaton()  # 初始化trie树
        for index, word in enumerate(wordlist):
            actree.add_word(word, (index, word))  # 向trie树中添加单词
        actree.make_automaton()  # 将trie树转化为Aho-Corasick自动机
        return actree



    '''问句过滤出 领域特征词'''
    def check_question(self, question):
        region_wds = []
        for i in self.region_tree.iter(question):  # ahocorasick 库 匹配问题  iter返回一个元组，i的形式如(3, (23192, 'huiji'))
            wd = i[1][1]  # 匹配到的词
            region_wds.append(wd)

        # print("region-words-tmp:",region_wds)

        stop_wds = []
        for wd1 in region_wds:
            for wd2 in region_wds:
                if wd1 in wd2 and wd1 != wd2:  # - 有重复字符串的领域词去除短的，取最长的领域词返回
                    stop_wds.append(wd1)  # stop_wds取重复的短的词，如region_wds=['乙肝', '肝硬化', '硬化']，则stop_wds=['硬化']
        final_wds = [i for i in region_wds if i not in stop_wds]  # final_wds取长词
        # parser_dict = {"".join(self.wdtype_dict.get(i)): [i] for i in final_wds}  # 获取词和词所对应的实体类型
        parser_dict = {i: "".join(self.wdtype_dict.get(i)) for i in final_wds} # 获取词和词所对应的实体类型
        # print(parser_dict)
        return parser_dict


    '''问答主函数'''
    def qa_main(self, question):
        final_dict = self.check_question(question)
        # print(final_dict)
        return final_dict


if __name__ == '__main__':
    handler = QuestionParser()
    while 1:
        question = input("用户：").strip()  # E-2C Hawkeye Group I的长度是什么
        handler.qa_main(question)

# 用户：E-2C Hawkeye Group I的长度是什么
# {'n_aircraft_name': ['E-2C Hawkeye Group I'], 'n_aircraft_attri': ['长度']}