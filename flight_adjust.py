# -*_coding:utf-8-*-
from datetime import datetime
from math import floor
from operator import itemgetter

from data_to_db import data_to_postgre as db
import pandas as pd
from log import logConfig

log = logConfig.get_log()
return_data = {}


def data_deal(AorD=None):
    all_data = db.read_from_db()
    all_data['start_scheme_takeoff_time'] = pd.to_datetime(all_data['start_scheme_takeoff_time'].apply(str))
    all_data['start_scheme_landin_time'] = pd.to_datetime(all_data['start_scheme_landin_time'].apply(str))
    if AorD == 'D':
        data = all_data.sort_values(by='start_scheme_takeoff_time')
    else:
        data = all_data.sort_values(by='start_scheme_landin_time')
    if AorD == 'A' or AorD == 'D':
        data = data[data['is_off_in'] == AorD]
    data.reset_index(drop=True, inplace=True)
    data['序号'] = [i for i in range(len(data))]
    data['new_time'] = [None] * len(data)
    data['use_or_not'] = [None] * len(data)
    data1 = data.copy()
    return data, data1


def data_deal_hours(AorD, all_data):
    if AorD == 'D':
        data = all_data.sort_values(by='start_scheme_takeoff_time')
    else:
        data = all_data.sort_values(by='start_scheme_landin_time')
    if AorD == 'A' or AorD == 'D':
        data = data[data['is_off_in'] == AorD]
    data.reset_index(drop=True, inplace=True)
    data['序号'] = [i for i in range(len(data))]
    data['new_time'] = [None] * len(data)
    data['use_or_not'] = [None] * len(data)
    data1 = data.copy()
    return data, data1


def time_reduction(AorD, rd_data, start_time, end_time, rate):
    if AorD == 'D':
        time_data = rd_data[
            (rd_data['start_scheme_takeoff_time'] >= start_time) & (rd_data['start_scheme_takeoff_time'] <= end_time)]
    else:
        time_data = rd_data[
            (rd_data['start_scheme_landin_time'] >= start_time) & (rd_data['start_scheme_landin_time'] <= end_time)]
    print("时段%s到时段%s,总共：%d班,总共调减%d班,还剩%d班" % (start_time, end_time, len(time_data),
                                              round(len(time_data) * rate),
                                              (len(time_data) - round(len(time_data) * rate))))
    return_data.update({'开始时间': start_time})
    return_data.update({'结束时间': end_time})
    return_data.update({'时段总航班量': len(time_data)})
    time_data.reset_index(drop=True, inplace=True)
    return time_data


def company_reduction(rd_data, rate=0):
    # 存放所有company_no
    list1 = list()
    company_type = []
    for x in rd_data['company_no']:
        list1.append(x)
    for x in list1:
        if x not in company_type:
            company_type.append(x)
    # 总共调减航班量
    company_count = 0
    # 每个company_no的所有航班
    company = [i for i in range(len(company_type))]
    # 每个company_no调减后的航班数
    company_number = company.copy()
    count = 0
    for i in company_type:
        company[count] = rd_data[rd_data['company_no'] == str(i)]
        count += 1

    for i in range(len(company_type)):
        company_number[i] = round(len(company[i]) * rate)
        company_count += company_number[i]
        print("航司%s共有:%d班，调减比例%.2f,调减了%d,调减后剩：%d,总共调减了:%d" % (
            company_type[i], len(company[i]), rate, company_number[i], len(company[i]) - company_number[i],
            company_count))
    return company_number, company, company_count, company_type


def reduction_flight(rd_data, rd_data1, rate):
    company_number, company, company_count, company_type = company_reduction(rd_data, rate)
    reduction_flight = list()  # 存放调减后的数据序号
    count = 0  # 计数器，记录第几次调减，可删除
    y = [i for i in range(len(rd_data))]  # 存放start_scheme_takeoff_time
    time_interval = y.copy()  # 存放航班间时间间隔
    for i in y:
        y[i] = rd_data['start_scheme_takeoff_time'][i]

    while 0 < company_count:
        rd_data.reset_index(drop=True, inplace=True)  # 重置data的index
        for i in range(len(time_interval)):
            if i == 0 or i == (len(time_interval) - 1):
                time_interval[i] = 1000
            else:
                time_interval[i] = int(
                    ((y[i] - y[i - 1]).total_seconds() / 60) + ((y[i + 1] - y[i]).total_seconds() / 60))
        print(time_interval)
        # sorted(enumerate(time_interval), key=itemgetter(1))
        index_num = [index for index, value in sorted(enumerate(time_interval), key=itemgetter(1))]  # 下标
        company_count -= 1
        flag = True
        for i in index_num:
            log.info("第%d次调减下标：%d,航司为： %s" % (count, i, rd_data["company_no"][i]))
            # print("第%d次调减下标：%d,航司为： %s,调减的序号值为： %s" % (count, i, rd_data["company_no"][i], flight_num[i]))
            count += 1
            for j in range(len(company_type)):
                if rd_data["company_no"][i] == company_type[j] and company_number[j] > 0:
                    company_number[j] -= 1
                    # print(data["company_no"][i], company_type[j], company_number[j])
                    reduction_flight.append(rd_data['序号'][i])
                    # print("调减company_no为%s,对应序号值为： %s 成功，调减后该航司还剩%d" % (company_type[j], flight_num[i], company_number[j]))
                    time_interval.pop(i)
                    y.pop(i)
                    rd_data.drop(index=i, axis=0, inplace=True)  # 根据index删除调减掉的航班一行数据
                    flag = False
                    break
                else:
                    continue
            if flag == False:
                break
    log.info(company_number)
    print(time_interval)
    print(len(reduction_flight), reduction_flight)
    for i in range(len(rd_data1)):
        if rd_data1['序号'][i] in reduction_flight:
            rd_data1['use_or_not'].values[i] = 0

        else:
            rd_data1['use_or_not'].values[i] = 1

    return rd_data1


def save_excel(rd_data, filename="测试结果.xlsx"):
    writer = pd.ExcelWriter(filename)
    # 去除多余的Unnamed:
    # data = data.loc[:, ~data.columns.str.contains("Unnamed:")]
    rd_data.to_excel(writer, '调减结果')
    writer.save()


def set_color(val):
    color = '#00EEEE' if val > 0 else 'grey'
    return 'background-color:%s' % color


def hours_adjust(data, AorD, start_time, end_time):
    after_adjust = pd.DataFrame()
    data['hours'] = data['start_scheme_takeoff_time'].dt.hour
    df_list = [group[1] for group in data.groupby(data['hours'])]  # 按小时切割数据
    time_list = pd.date_range(start=start_time, end=end_time, freq='H')
    time_list = pd.Series(time_list)
    hour_list = time_list.dt.hour
    for i in hour_list:
        for time_hour_data in df_list:
            temp_data = time_hour_data.loc[time_hour_data['hours'] == i]
            if not temp_data.empty:
                rd_data, rd_data1 = data_deal_hours(AorD, temp_data)
                result_data = reduction_flight(rd_data, rd_data1, 0.3)
                after_adjust = after_adjust.append(result_data, ignore_index=True)
    return after_adjust


# time_data = time_reduction('D', data, '2021-07-09 11:00:00', '2021-07-09 20:00:00', 0.3)
# data = company_reduction(time_data, rate=0.3)
# data = reduction_flight(data, data1, 0.3)
# print(data)
# save_excel(time_data, filename="测试结果.xlsx")


# 每小时航班量统计
# pd.to_datetime(data['start_scheme_takeoff_time'])

# print(data['hours'])
# data['new_data'] = data['start_scheme_takeoff_time']
# data = pd.to_datetime(data['start_scheme_takeoff_time'])
# data.set_index(['start_scheme_takeoff_time'], inplace=True)
# 按小时划分数据集：先将字符串时间段转为时间类型数据时间段作为index,然后用这个时间段的小时作为切分的依据
# 发现不需要把时间作为index才能分组，直接指定某个列作为分组就可以将datafram划分为多个小的datafram了，前提就是要先将那个时间字段先转为时间字段，然后再提取这个时间字段
# 的年、月、日、时、分、秒，选择分组的维度，就可以了,然后将切割的datafram放到一个list里面

# data, data1 = data_deal('D')
# after_adjust = hours_adjust(data, 'D', '2021-07-09 11:00:00', '2021-07-09 21:00:00')
# after_adjust = after_adjust.style.applymap(set_color, subset='use_or_not')
# save_excel(after_adjust, filename="测试结果.xlsx")

data, data1 = data_deal('D')
data = time_reduction('D', data, '2021-09-09 01:00:00', '2021-09-09 23:00:00', 0.3)
data = data[data['terminal_station'] == 'SHA']
data['hours'] = data['start_scheme_takeoff_time'].dt.hour
df_list = [group[1] for group in data.groupby(data['hours'])]  # 按小时切割数据
time_list = pd.date_range(start='2021-09-09 01:00:00', end='2021-09-09 23:00:00', freq='H')
time_list = pd.Series(time_list)
hour_list = time_list.dt.hour
# for i in df_list:
#     print(i['start_scheme_takeoff_time'].dt.hour.iloc[0], type(i['start_scheme_takeoff_time'].dt.hour.iloc[0]), len(i))
for i in hour_list:
    for time_hour_data in df_list:
        temp_data = time_hour_data.loc[time_hour_data['hours'] == i]
        if not temp_data.empty:
            print(time_hour_data['start_scheme_takeoff_time'].dt.hour.iloc[0], time_hour_data['start_scheme_takeoff_time'])
