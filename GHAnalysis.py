import json
import os
import argparse
import time
import mmap
from multiprocessing import Pool, cpu_count
import shutil

class Data:
    def __init__(self, dict_address: str = None, reload: int = 0):
        if reload == 1:
            try:
                os.makedirs('json_temp')
            except:
                shutil.rmtree('json_temp')
                os.makedirs('json_temp')
            self.__init(dict_address)

        if dict_address is None and \
                not os.path.exists('1.json') and not os.path.exists('2.json') and not os.path.exists('3.json'):
            raise RuntimeError("Error: init failed")

        x = open('1.json', 'r', encoding='utf-8').read()
        self.__4Events4PerP = json.loads(x)
        x = open('2.json', 'r', encoding='utf-8').read()
        self.__4Events4PerR = json.loads(x)
        x = open('3.json', 'r', encoding='utf-8').read()
        self.__4Events4PerPPerR = json.loads(x)

    def __init(self, dict_address: str):
        start_time = time.time()
        self.__4Events4PerP = {}
        self.__4Events4PerR = {}
        self.__4Events4PerPPerR = {}

        for root, dic, files in os.walk(dict_address):
            pool = Pool(processes=max(cpu_count(), 6))
            for f in files:
                if f[-5:] == '.json':
                    pool.apply_async(self.readFile, args=(f, dict_address))
            pool.close()
            pool.join()
        # print("read file:",time.time()-start_time)

        for root, dic, files in os.walk("json_temp"):
            for f in files:
                if f[-5:] == '.json':
                    json_list = json.loads(open("json_temp" + '\\' + f, 'r', encoding='utf-8').read())
                    for item in json_list:
                        if not self.__4Events4PerP.get(item['actor__login'], 0):
                            self.__4Events4PerP.update({item['actor__login']: {}})
                            self.__4Events4PerPPerR.update({item['actor__login']: {}})
                        self.__4Events4PerP[item['actor__login']][item['type']] = \
                            self.__4Events4PerP[item['actor__login']].get(item['type'], 0) + 1

                        if not self.__4Events4PerR.get(item['repo__name'], 0):
                            self.__4Events4PerR.update({item['repo__name']: {}})
                        self.__4Events4PerR[item['repo__name']][item['type']] = \
                            self.__4Events4PerR[item['repo__name']].get(item['type'], 0) + 1

                        if not self.__4Events4PerPPerR[item['actor__login']].get(item['repo__name'], 0):
                            self.__4Events4PerPPerR[item['actor__login']].update({item['repo__name']: {}})
                        self.__4Events4PerPPerR[item['actor__login']][item['repo__name']][item['type']] = \
                            self.__4Events4PerPPerR[item['actor__login']][item['repo__name']].get(item['type'], 0) + 1
        # print("count:",time.time()-start_time)
        with open('1.json', 'w', encoding='utf-8') as f:
            json.dump(self.__4Events4PerP, f)
        with open('2.json', 'w', encoding='utf-8') as f:
            json.dump(self.__4Events4PerR, f)
        with open('3.json', 'w', encoding='utf-8') as f:
            json.dump(self.__4Events4PerPPerR, f)
        # print("write file:",time.time()-start_time)


    def saveJson(self, json_list, filename):
        batch_message = []
        for item in json_list:
            if item['type'] not in ["PushEvent", "IssueCommentEvent", "IssuesEvent", "PullRequestEvent"]:
                continue
            batch_message.append({'actor__login': item['actor']['login'], 'type': item['type'], 'repo__name': item['repo']['name']})
        with open('json_temp\\' + filename, 'w', encoding='utf-8') as F:
            json.dump(batch_message, F)


    def readFile(self, f, dict_address):
        json_list = []
        if f[-5:] == '.json':
            json_path = f
            x = open(dict_address + '\\' + json_path, 'r', encoding='utf-8')
            with mmap.mmap(x.fileno(), 0, access=mmap.ACCESS_READ) as m:
                m.seek(0, 0)
                obj = m.read()
                obj = str(obj, encoding="utf-8")
                str_list = [_x for _x in obj.split('\n') if len(_x) > 0]
                for _str in str_list:
                    try:
                        json_list.append(json.loads(_str))
                    except:
                        pass
            self.saveJson(json_list, f)


    def getEventsUsers(self, username: str, event: str) -> int:
        if not self.__4Events4PerP.get(username,0):
            return 0
        else:
            return self.__4Events4PerP[username].get(event,0)

    def getEventsRepos(self, reponame: str, event: str) -> int:
        if not self.__4Events4PerR.get(reponame,0):
            return 0
        else:
            return self.__4Events4PerR[reponame].get(event,0)

    def getEventsUsersAndRepos(self, username: str, reponame: str, event: str) -> int:
        if not self.__4Events4PerP.get(username,0):
            return 0
        elif not self.__4Events4PerPPerR[username].get(reponame,0):
            return 0
        else:
            return self.__4Events4PerPPerR[username][reponame].get(event,0)


class Run:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.data = None
        self.argInit()
        t1 = time.time()
        print(self.analyse())
        t2 = time.time()

    def argInit(self):
        self.parser.add_argument('-i', '--init')
        self.parser.add_argument('-u', '--user')
        self.parser.add_argument('-r', '--repo')
        self.parser.add_argument('-e', '--event')

    def analyse(self):
        if self.parser.parse_args().init:
            self.data = Data(self.parser.parse_args().init, 1)
            return 0
        else:
            if self.data is None:
                self.data = Data()
            if self.parser.parse_args().event:
                if self.parser.parse_args().user:
                    if self.parser.parse_args().repo:
                        res = self.data.getEventsUsersAndRepos(
                            self.parser.parse_args().user, self.parser.parse_args().repo, self.parser.parse_args().event)
                    else:
                        res = self.data.getEventsUsers(
                            self.parser.parse_args().user, self.parser.parse_args().event)
                elif self.parser.parse_args().repo:
                    res = self.data.getEventsRepos(
                        self.parser.parse_args().repo, self.parser.parse_args().event)
                else:
                    raise RuntimeError("Error: argument -u|--user or -r|--repo cannot be found!")
            else:
                raise RuntimeError("Error: argument -e|--event cannot be found!")
        return res

if __name__ == '__main__':
    a = Run()
