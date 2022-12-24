个  人  简  介
 





 

姓       名：余天养
籍       贯：广东韶关
联系电话：15019470737
工作年限：6年
 





 

年       龄：29 岁
性       别：男
学       历：本科
邮       箱：353871729@qq.com 
教  育  背  景 

三明学院
 

2012- 2016 
求  职  意  向
工作性质：全职
目标地点：深圳
目标薪资： 

专  业  技  能
1、	熟悉Java、python基础
2、	熟练掌握数据库相关操作,比如 Postgresql、Mysql 关系型数据库
3、	了解大数据相关组件、HCS、MRS、DGC、DataArk
4、	能够使用简单的 linux 命令，对 linux 进行简单操作；

 

工  作  经  历
工作时间	工作单位	职位名称
2017年 1 月-2019 年 8月	SGS技术服务有限公司	Java 开发工程师
2019 年8月-2020年5月	中软国际	解决方案、产品数据开发工程师
2020年6月-至今            科锐国际                                解决方案、大数据开发工程师

项  目  经  验
项目一：解决方案架构师助理
项目角色：大数据解决方案架构师助理
项目名称：深圳地铁NOCC二期管理云项目
项目描述：该项目是在深圳数字地铁一期项目基础上进行扩展，进一步加强深圳地铁数字化转型，新建智慧运维、乘客服务、应 急指挥、移动办公等系统，新增30多个业务系统数据集成、数据治理及相关专题库数据的建设。
工作内容：
1、需求调研：与客户会议确定数据需求
2、HCS云平台升级、应用迁移方案交流、设计
3、审核、跟进伙伴（东方国信）数据调研方案、数据集成、数据治理、数据实施方案等，问题及时反馈SA

项目二：放行协同
项目名称：航班调减、调时
开发环境：python3.8、Flask、高斯数据库
项目描述：主要解决运行指挥中心应用系统多、数据分散、质量差、缺乏预测预警功能等问题，
减轻指挥员的工作压力和强度
责任描述：
1.	航班调减、航班调时
1.1、	需求分析（理解分析客户需求）
1.2、	业务研讨（认识真实业务，深入理解客户的需求）
1.3、	数据分析（基于真实数据分析，了解客户当前的处理方式）
1.4、	开发、测试、上线（将客户工作程序化）

项目三：解决方案配置报价开发
项目名称：智慧机场解决方案配置器开发、标准型平安城市解决方案配置器开发
开发环境：CRS、CB、DPM	
项目描述：解决方案配置器主要给华为一线产品经理进行配置、报价、及生成产品报价单，
用于解决方案报价预算。项目有由数据、eDesinger、SCT等多个项目组协同完成。
责任描述：
1	智慧机场IOC子系统开发上线
2.	智慧机场大数据子系统开发上线
3.	出行一张脸子系统开发上线
4.	标准型DC一层模块开发
5.	标准型平安城市SCT1.0迁移2.0
6.	标准型平安城市2.0大数据、DC一层、统一安全数据参数优化

项目四
项目名称：集团项目管理系统
开发环境：IntelliJ IDEA  +Mysql5.6+Tomcat8+GIT+JDK1.8	
技术架构：前端：VUE+ElementUI 后端： springboot + mybatis
项目描述：集团项目管理系统是招商金科内部使用的项目管理工具，系统可以创建管理立项项目和合同项目。
可以在系统中查看各项目的详细信息及存在风险等功能。
责任描述：1.负责立项项目和合同项目项目成员的增删改查。
          2.负责合同项目的创建。
          3.负责项目里程碑的创建。

自  我  评  价

本人为人诚恳、乐观向上、热爱学习；
富有团队精神和团队意识，责任心强；
具有较强的逻辑思维能力与判断能力



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
