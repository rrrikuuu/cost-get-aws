import boto3
import json
import logging
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, date, timedelta
from decimal import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ce = boto3.client('ce')

# 固定の為替レートを設定
price = 110  # 例として、1ドルあたり110円と仮定

# パラメータストアからwebhookurlを取得する
def get_ssm_params(*keys, region='ap-northeast-1'):
    result = {}
    ssm = boto3.client('ssm', region)
    response = ssm.get_parameters(
        Names=keys,
        WithDecryption=True,
    )

    for p in response['Parameters']:
        result[p['Name']] = p['Value']

    return result
    
parameters = get_ssm_params('SLACK_URL')
SLACK_CHANNEL     = os.environ['SLACK_CHANNEL']
SLACK_WEBHOOK_URL = parameters['SLACK_URL']
AWS_ACCOUNT_NAME  = os.environ['AWS_ACCOUNT_NAME']

# 対象月にかかったAWSの合計金額の算出
def get_total_billing(client) -> dict:
    # コスト集計範囲の取得
    (start_date, end_date) = get_total_cost_date_range()

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ce.html#CostExplorer.Client.get_cost_and_usage
    # コスト集計範囲に対する合計コストの取得
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost'
        ]
    )
    # 取得したデータの返却
    return {
        'start': response['ResultsByTime'][0]['TimePeriod']['Start'],
        'end': response['ResultsByTime'][0]['TimePeriod']['End'],
        'billing': response['ResultsByTime'][0]['Total']['AmortizedCost']['Amount'],
    }
    
# 対象月のコスト算出対象の初日と当日の日付を取得する
def get_total_cost_date_range() -> (str, str):

    # Costを算出する期間を設定する
    start_date = get_begin_of_month()
    end_date = get_today()

    # 「start_date」と「end_date」が同じ場合、「start_date」は先月の月初の値を取得する。
    if start_date == end_date:
        end_of_month = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=-1)
        begin_of_month = end_of_month.replace(day=1)
        return begin_of_month.date().isoformat(), end_date
    return start_date, end_date

def get_begin_of_month() -> str:
    return date.today().replace(day=1).isoformat()

def get_prev_day(prev: int) -> str:
    return (date.today() - timedelta(days=prev)).isoformat()

def get_today() -> str:
    return date.today().isoformat()    

def lambda_handler(event, context):
    logger.info("Event: %s", str(event))

    (start_date, end_date) = get_total_cost_date_range()

    fields = []
    total  = Decimal(0)

    ce_res = ce.get_cost_and_usage(
        TimePeriod={
            'Start': str(start_date),
            'End': str(end_date)
        },
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[{
            'Type': 'DIMENSION',  # タグの値別で内訳を出す場合はTypeにTAG、Keyにタグ名を入れる
            'Key': 'SERVICE'  # アカウント別で内訳を出す場合はKeyにLINKED_ACCOUNTを入れる
        }]
    )
    logger.info("CostAndUsage: %s", str(ce_res))
    
    rbt = ce_res['ResultsByTime']
    for groups in rbt:
        for group in groups['Groups']:
            
            value = round(Decimal(group["Metrics"]["UnblendedCost"]["Amount"]) * price, 0)
            # 情報量が多いため、2円以下は割愛
            if value >= 2:
                print(value)
                fields.append({
                    "title": group["Keys"][0],
                    "value": ":yen: " + str("{:,}".format(value)) + "円",
                    "short": True
                })
                total += Decimal(group["Metrics"]["UnblendedCost"]["Amount"])
            else:
                logger.info("Event: %s is under 1 yen", group["Keys"][0])
    
    # Slack通知内容の作成
    slack_message = {
        "attachments": [
            {
                'fallback': "Required plain-text summary of the attachment.",
                'color': "#36a64f",
                'author_name': ":male-police-officer: :moneybag: Billing 警察",
                'author_link': "https://us-east-1.console.aws.amazon.com/cost-management/home?region=ap-northeast-1#/cost-explorer?chartStyle=STACK&costAggregate=unBlendedCost&endDate=2023-10-31&excludeForecasting=false&filter=%5B%5D&futureRelativeRange=CUSTOM&granularity=Monthly&groupBy=%5B%22Service%22%5D&historicalRelativeRange=LAST_6_MONTHS&isDefault=true&reportName=%E6%96%B0%E3%81%97%E3%81%84%E3%82%B3%E3%82%B9%E3%83%88%E3%81%A8%E4%BD%BF%E7%94%A8%E7%8A%B6%E6%B3%81%E3%83%AC%E3%83%9D%E3%83%BC%E3%83%88&showOnlyUncategorized=false&showOnlyUntagged=false&startDate=2023-05-01&usageAggregate=undefined&useNormalizedUnits=false",
                'text': "<!here>【サービス別】",
                'fields': fields,
                'footer': "Powered by on %s Lambda" % (str(AWS_ACCOUNT_NAME)),
                'footer_icon': "https://platform.slack-edge.com/img/default_application_icon.png",
                'pretext': "* %s~%s の [ %s ] のAWS利用料金 は :money_with_wings: %s 円です ※本日の為替レート[%s 円/1ドル]で計算しています*" % (str(start_date), str(end_date), str(AWS_ACCOUNT_NAME), str("{:,}".format(round(total * price, 0))), str(price)),
                'channel': SLACK_CHANNEL
            }
        ]
    }
    
    # Slackへの通知
    req = Request(SLACK_WEBHOOK_URL, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['attachments'][0]['channel'])
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)