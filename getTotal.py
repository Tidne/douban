import requests
import csv
import random
from bs4 import BeautifulSoup
import re
import time


def get_header():
    # 从ua.csv中随机读取200个ua,以列表形式返回
    headers = []
    f = open("ua_string.csv", "r", encoding="utf-8")
    allHeaders = csv.reader(f)
    for index, header in enumerate(allHeaders):
        headers.append(header)
        if index == 200:
            f.close()
            break
    return headers


class TypeDB():
    # 从主页面中爬取电影分类id, 并获得每个分类对应的层次的数量，写入到type_total.csv中
    def __init__(self):
        self.totalUrl = "https://movie.douban.com/j/chart/top_list_count?type={}&interval_id={}"
        self.allType = "https://movie.douban.com/chart"
        self.all_headers = get_header()
        self.pattern = re.compile(".*?type_name=(.*?)&type=(.*?)&inter.*", re.S)
        self.headers = random.choice(self.all_headers)[0]
        self.header = {
            "user-agent": self.headers,
            "host": 'movie.douban.com',
        }

    def get_type(self):
        # 从主页中解析所有分类Id, 并将id传给get_count()调用,接收其返回的每个分类的数据,
        # 再调用write_type()写入type_total.csv中
        html = requests.get(self.allType,headers=self.header)
        html = html.text
        soup = BeautifulSoup(html, "lxml")
        types = soup.find("div", {"class": "types"})
        a = types.find_all('a')
        for i in a:
            try:
                href = i["href"]
                result = re.findall(self.pattern, href)
                for one in result:
                    type_name, type_id = one
                    totals = self.get_count(type_id)
                    x = [type_id,type_name]
                    x.extend(totals)
                    self.write_type(x)
            except Exception as e:
                print(e)
        print("所有分类总数获取完成，开始爬取电影信息")

    def get_count(self,type_id):
        # 接收get_type()传过来的分类id,获取每个层次的电影总数,形成列表返回
        rate = ['100%3A90', '90%3A80', '80%3A70', '70%3A60', '60%3A50', '50%3A40', '40%3A30', '30%3A20', '20%3A10',
                '10%3A0']
        totals = []
        for i in rate:
            try:
                data = requests.get(self.totalUrl.format(type_id, i), headers=self.header)
                data = data.json()
                total = data.get("total")
                totals.append(total)
                time.sleep(10)
            except Exception as e:
                print(e)
        return totals

    def write_type(self,data):
        # 接收get_type()传过来的关于每个电影分类的数据，存到type_total.csv中
        # 首次调用并传入空值，即可写入title
        if data:
            f = open("type_total.csv", "a", encoding="utf-8", newline='')
            writer = csv.writer(f)
            writer.writerow(data)
            print("{} 数据总数统计完成".format(data[1]))
            f.close()
        else:
            f = open("type_total.csv", "a", encoding="utf-8", newline='')
            writer = csv.writer(f)
            writer.writerow(['type_id', 'type_name', '100-90', '90-80', '80-70', '70-60', '60-50', '50-40',
                             '40-30', '30-20', '20-10', '10-0'])
            f.close()


class MovieDB():
    # 读取type_total.csv中的信息,爬取电影信息数据,并分类别写入到csv文件中
    def __init__(self):
        self.url = 'https://movie.douban.com/j/chart/top_list?type={}&interval_id={}&action=&start={}&limit=20'
        self.all_headers = get_header()
        self.pattern = re.compile(".*?type_name=(.*?)&type=(.*?)&inter.*", re.S)
        self.headers = random.choice(self.all_headers)[0]
        self.header = {
            "user-agent": self.headers,
            "host": 'movie.douban.com',
        }

    def read_total(self):
        # 读取type_total.csv每一行,将有效数据传给get_movie()进行爬取(每次传入一个类别的数据)
        f = open("type_total.csv", "r", encoding="utf-8")
        allType = csv.reader(f)
        for index,oneType in enumerate(allType):
            if index != 0:
                self.get_movie(oneType)

    def get_movie(self,lists):
        # 接收read_total()传来的参数,分层次进行爬取,每爬取到一页,便调用write_movie()写入
        type_id = int(lists[0])
        type_name = lists[1]
        rate = ['100%3A90', '90%3A80', '80%3A70', '70%3A60', '60%3A50', '50%3A40', '40%3A30', '30%3A20', '20%3A10',
                '10%3A0']
        for i in range(len(rate)):
            j = lists[2+i]
            j = int(j)
            page = j//20 + (1 if j % 20 != 0 else 0)
            x = 0
            while x<page:
                try:
                    data = requests.get(self.url.format(type_id, rate[i], x*20), headers=self.header)
                    time.sleep(15)
                    data = data.json()
                    self.write_movie(type_name, rate[i], data)
                    print("type:{} page:{} 写入完成".format(type_name, x))
                    x += 1
                except Exception as e:
                    print(e)
                    print("类别{} 第{}页爬取失败".format(type_name, x))
                    x += 1
            print("type:{} {}写入完成".format(type_name, rate[i]))
        print("{} 所有页面写入完成".format(type_name))
        time.sleep(3600)

    def write_movie(self,type_name,rate,data):
        # 接收get_movie()传来的参数,将数据按规范写入csv文件中,不写title
        if data:
            f = open("{}_{}.csv".format(type_name,rate.replace('%3A','-')), "a", encoding="utf-8", newline='')
            writer = csv.writer(f)
            for i in data:
                values = list(i.values())[:-1]
                writer.writerow(values)
            f.close()
        else:
            # f = open("movie.csv","a",encoding="utf-8",newline='')
            # writer = csv.writer(f)
            # writer.writerow(['rating','rank','coverUrl','isPlayable','id','types','regions','title',
            #                  'url','releaseDate','actorCount','voteCount','score','actors'])
            # f.close()
            # print("title写入完成")
            print("data为空")


if __name__ == "__main__":
    td = TypeDB()
    td.get_type()
    demo = MovieDB()
    demo.read_total()