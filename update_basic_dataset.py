
from pymongo import MongoClient
import os
import datetime
import time
import logging
from pytz import timezone, utc
import pymysql
import pandas as pd


class UpdateCostRevenue:

    def __init__(self):
        self.db_host = os.environ['db_host']
        self.db_port = os.environ['db_port']
        self.db_user = os.environ['db_user']
        self.db_name = os.environ['db_name']
        self.db_pwd = os.environ['db_pwd']
        self.report_name = 'report'
        self.ads_name = 'ads'
        self.recent_date = datetime.date.today()
        self.cycle_day = 0
        self.logger = self.logger_conf()
        self.reports = dict()
        self.basictable = dict()
        self.mysql_db_host = 'localhost'
        self.mysql_db_port = 3306
        self.mysql_db_user = 'root'
        self.mysql_db_pwd = ''
        self.mysql_db_name = 'mytest'
        self.mysql_table_name = 'update_data'

    def custom_time(*args):
        # 配置logger
        utc_dt = utc.localize(datetime.datetime.utcnow())
        my_tz = timezone("Asia/Shanghai")
        converted = utc_dt.astimezone(my_tz)
        return converted.timetuple()

    def logger_conf(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.INFO)
        if os.path.exists(r'./logs') == False:
            os.mkdir('./logs')
            if os.path.exists('./update_cost_revenue_log.txt') == False:
                fp = open("./logs/update_cost_revenue_log.txt", 'w')
                fp.close()
        handler = logging.FileHandler("./logs/update_cost_revenue_log.txt", encoding="UTF-8")
        handler.setLevel(logging.INFO)
        logging.Formatter.converter = self.custom_time
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    ''' connect mongodb '''
    def mongodb_conn(self):
        client = MongoClient(host=self.db_host, port=int(self.db_port))
        db = client.get_database(self.db_name)
        db.authenticate(self.db_user.strip(), self.db_pwd.strip())
        return db, client

    ''' connect mysql '''
    def mysql_conn(self):
        # conn = pymysql.connect(host='localhost', port=3306, db='mytest', user='root', passwd='', charset='utf8')
        conn = pymysql.connect(host=self.mysql_db_host, port=self.mysql_db_port, user=self.mysql_db_user,
                             passwd=self.mysql_db_pwd, db=self.mysql_db_name, charset='utf8')
        return conn

    ''' str to date '''
    def string_to_datetime(self):
        return datetime.datetime.strptime(self.recent_date, "%Y-%m-%d")

    ''' find a recent date of recent cohort_date '''
    def find_report_recent_date(self, db):
        self.logger.info('get recent date from report collection')
        colles_report_date = db.get_collection(self.report_name).find({}, {'_id': 0, 'cohort_date': 1})\
            .sort([('cohort_date', -1)]).limit(1)
        for date in colles_report_date:
            self.recent_date = date['cohort_date']
        self.recent_date = self.string_to_datetime()
        self.recent_date = datetime.datetime.date(self.recent_date+datetime.timedelta(days=-self.cycle_day))
        self.logger.info('recent date is '+str(self.recent_date))

    ''' according to recent cohort_date to find data from report '''
    def find_report(self, db):
        print(self.recent_date)
        colles_report = db.get_collection(self.report_name).find({'cohort_date': str(self.recent_date), 'cost': {'$gt': 0}})
        for item in colles_report:
            self.reports[item['ad_id']] = {'country': item['country'],
                                           'platform': item['platform'],
                                           'install': item['install'],
                                           'pay': item['pay'],
                                           'cost': item['cost'],
                                           'revenue_day1': item['revenue_day1'],
                                           'revenue_day2': item['revenue_day2'],
                                           'revenue_day3': item['revenue_day3'],
                                           'revenue_day4': item['revenue_day4'],
                                           'revenue_day5': item['revenue_day5'],
                                           'revenue_day6': item['revenue_day6'],
                                           'revenue_day7': item['revenue_day7']
                                          }

    ''' get some ad_ids from report in recent cohort_date, to find ads from ads collection '''
    def find_ads(self, db):
        adids = list(self.reports.keys())
        colles_ads = db.get_collection(self.ads_name).find({'ad_id': {'$in': adids}}, {'_id': 0, 'ad_id': 1, 'pt': 1})
        tmp_adids = []
        no_behaviors = []
        no_interests = []
        for ad in colles_ads:
            tmp_adids.append(ad['ad_id'])
            if ad.get('pt') and ad['pt'].get('adset_spec') and ad['pt']['adset_spec'].get('targeting') and \
                    ad['pt']['adset_spec']['targeting'].get('behaviors'):
                behaviors = ad['pt']['adset_spec']['targeting']['behaviors']
                if isinstance(behaviors, dict):
                    for value in behaviors.values():
                        if value.get('id'):
                            self.add_basic_table(ad['ad_id'], value['id'], 'behavior')
                elif isinstance(behaviors, list):
                    for value in behaviors:
                        if isinstance(value, dict) and value.get('id'):
                            self.add_basic_table(ad['ad_id'], value['id'], 'behavior')
            else:
                no_behaviors.append(ad['ad_id'])

            if ad.get('pt') and ad['pt'].get('adset_spec') and ad['pt']['adset_spec'].get('targeting') and \
                    ad['pt']['adset_spec']['targeting'].get('interests'):
                interests = ad['pt']['adset_spec']['targeting']['interests']
                if isinstance(interests, dict):
                    for value in interests.values():
                        if value.get('id'):
                            self.add_basic_table(ad['ad_id'], value['id'], 'interest')
                elif isinstance(interests, list):
                    for value in interests:
                        if isinstance(value, dict) and value.get('id'):
                            self.add_basic_table(ad['ad_id'], value['id'], 'interest')
            else:
                no_interests.append(ad['ad_id'])
        not_adids = [xx for xx in tmp_adids if xx not in adids]
        self.logger.info('ad_id from report has size of not behaviors '+str(len(no_behaviors)))
        self.logger.info('ad_id from report has size of not interests ' + str(len(no_interests)))
        if len(not_adids) > 0:
            self.logger.info('ad_id in report not in ads ['+','.join(not_adids)+']')
        else:
            self.logger.info('ad_id in report not in ads is zero')
        # print(self.basictable)

    ''' sum value of install,pay,cost,revenue from report by same country,platform,value'''
    def add_basic_table(self, ad_id, kyd, category):
        rp = self.reports[ad_id]
        ky = str(kyd)+'_' + rp['country'] + '_' + rp['platform']
        if ky not in self.basictable:
            self.basictable[ky] = {'id': str(),
                                   'cohort_date': str(),
                                   'ad_id': set(),
                                   'country': str(),
                                   'platform': str(),
                                   'install': 0,
                                   'pay': 0,
                                   'cost': 0,
                                   'revenue_day1': 0,
                                   'revenue_day2': 0,
                                   'revenue_day3': 0,
                                   'revenue_day4': 0,
                                   'revenue_day5': 0,
                                   'revenue_day6': 0,
                                   'revenue_day7': 0
                                   }
        self.basictable[ky]['id'] = str(kyd)
        self.basictable[ky]['cohort_date'] = str(self.recent_date)
        self.basictable[ky]['ad_id'].add(ad_id)
        self.basictable[ky]['country'] = rp['country']
        self.basictable[ky]['platform'] = rp['platform']
        self.basictable[ky]['install'] += rp['install']
        self.basictable[ky]['pay'] += rp['pay']
        self.basictable[ky]['cost'] = round(self.basictable[ky]['cost'] + rp['cost'], 2)
        self.basictable[ky]['revenue_day1'] = round(self.basictable[ky]['revenue_day1'] + rp['revenue_day1'], 2)
        self.basictable[ky]['revenue_day2'] = round(self.basictable[ky]['revenue_day2'] + rp['revenue_day2'], 2)
        self.basictable[ky]['revenue_day3'] = round(self.basictable[ky]['revenue_day3'] + rp['revenue_day3'], 2)
        self.basictable[ky]['revenue_day4'] = round(self.basictable[ky]['revenue_day4'] + rp['revenue_day4'], 2)
        self.basictable[ky]['revenue_day5'] = round(self.basictable[ky]['revenue_day5'] + rp['revenue_day5'], 2)
        self.basictable[ky]['revenue_day6'] = round(self.basictable[ky]['revenue_day6'] + rp['revenue_day6'], 2)
        self.basictable[ky]['revenue_day7'] = round(self.basictable[ky]['revenue_day7'] + rp['revenue_day7'], 2)
        self.basictable[ky]['category'] = category

    ''' read local dates which have been updated before'''
    def read_rencent_day(self):
        if os.path.exists('update_recent_date.txt'):
            data = pd.read_csv('update_recent_date.txt', header=None)
            recent_dates = list(data.iloc[:, 0])
            if str(self.recent_date) in recent_dates:
                return False
        return True

    ''' save date which has been updated successfully now '''
    def save_recent_date(self):
        with open("update_recent_date.txt", 'a+') as fopen:
            fopen.write(str(self.recent_date) + '\n')

    ''' insert data to mysql '''
    def save_update_data_mysql(self):
        conn = self.mysql_conn()
        cursor = conn.cursor()
        nums = cursor.execute('''Create table if not exists {}(
                                        `_id` int(11) NOT NULL AUTO_INCREMENT,
                                        `id`	varchar(30) NOT NULL,
                                        `cohort_date` varchar(11),
                                        `ads_count` int(5) DEFAULT 0,
                                        `country` varchar(64),
                                        `platform` varchar(32),
                                        `install_count` int(10) DEFAULT 0,
                                        `pay_count` int(10) DEFAULT 0,
                                        `total_cost` float DEFAULT 0,
                                        `total_revenue_day1` float DEFAULT 0,
                                        `total_revenue_day2` float DEFAULT 0,
                                        `total_revenue_day3` float DEFAULT 0,
                                        `total_revenue_day4` float DEFAULT 0,
                                        `total_revenue_day5` float DEFAULT 0,
                                        `total_revenue_day6` float DEFAULT 0,
                                        `total_revenue_day7` float DEFAULT 0,
                                        `category` varchar(20),
                                        primary key(`_id`)
                                        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;'''.format(self.mysql_table_name))
        if nums > 0:
            self.logger.info('the update table has already benn exists in mysql')
        else:
            self.logger.info('create update data table in mysql')
        update_lst = []
        iii = 0
        sql = "insert into  {}(id,cohort_date,ads_count,country,platform,install_count,pay_count,total_cost," \
              "total_revenue_day1,total_revenue_day2,total_revenue_day3,total_revenue_day4,total_revenue_day5," \
              "total_revenue_day6,total_revenue_day7,category) values " \
              "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.mysql_table_name)
        for key, values in self.basictable.items():
            print(key)
            print(values)
            dt = (values['id'], values['cohort_date'], str(len(values['ad_id'])), values['country'],
                  values['platform'], str(values['install']), str(values['pay']), str(values['cost']),
                  str(values['revenue_day1']), str(values['revenue_day2']), str(values['revenue_day3']), str(values['revenue_day4']),
                  str(values['revenue_day5']), str(values['revenue_day6']), str(values['revenue_day7']), values['category'])
            update_lst.append(dt)

            if iii > 5000:
                cursor.executemany(sql, update_lst)
                conn.commit()
                iii = 0
                update_lst.clear()
            iii += 1
        if iii > 0:
            cursor.executemany(sql, update_lst)
            conn.commit()
        cursor.close()
        conn.close()
        self.logger.info('insert into '+self.mysql_table_name+' total size ' + str(len(self.basictable.keys())))

    ''' main '''
    def main(self):
        self.logger.info('get mongodb connection.')
        db, client = self.mongodb_conn()
        self.logger.info('already connect mongodb.')
        self.find_report_recent_date(db)
        if self.read_rencent_day():
            self.find_report(db)
            self.find_ads(db)
            self.save_update_data_mysql()
            self.logger.info('update end')
            self.save_recent_date()
            client.close()
            return 'updated successfully!'
        else:
            self.logger.info(str(self.recent_date)+' has already updated! No need to update!')
            client.close()
            return 'It has been update before, No need to update'


if __name__ == '__main__':
    ucr = UpdateCostRevenue()
    t = time.time()
    ucr.main()
    # ucr.check_ads()
    print(time.time()-t)
    # ucr.find_report()

