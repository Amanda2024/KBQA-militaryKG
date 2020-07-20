# coding: utf-8
# Author: rjq<ruanjingqing@126.com>
# Date: 2020-07-18

from question_parser import *
from parser_cypher_search import *

'''问答类'''
class ChatBotGraph:
    def __init__(self):
        self.parser = QuestionParser()
        self.searcher = AnswerSearcher()

    def chat_main(self, sent):
        answer = '您好，我是Military小助手，希望可以帮到您：'
        res_dict = self.parser.qa_main(sent)
        # {'E-2C Hawkeye Group I':'n_aircraft_name','长度': 'n_aircraft_attri'}
        print("First,解析出实体属性：",res_dict)
        if not res_dict:
            return '抱歉，小助手暂时无法回答您的问题。'
        print('Second,转换成Cypher查询：', end='')
        final_answers = self.searcher.parse2cypher2answer(res_dict)
        if not final_answers:
            return answer
        else:
            return final_answers

if __name__ == '__main__':
    handler = ChatBotGraph()
    print('请等待模型初始化……')
    while 1:
        question = input('用户:')  # E-2C Hawkeye Group I的长度是什么
        answer = handler.chat_main(question)
        print('小助手:', answer)
















