import argparse
import csv
import datetime

import logging
import os
import pathlib
import shutil
import sys
import traceback
from faker import Faker
from mimesis import Cryptographic
from mimesis.random import Random
from mimesis.schema import Field, Schema
import random
import json


class DataGenerator:

    def __init__(self, root_output_path):
        self.root_output_path = root_output_path

    def generate_purchases(self, size=10000):
        fake = Faker()
        _ = Field('en')
        purchases = (
            lambda: {
                'purchaseId': _('uuid'),
                'purchaseTime': fake.date_time_between(start_date='-100d', end_date='now').strftime(
                    "%Y-%m-%d %H:%M:%S"),
                'billingCost': _('price')[1:],
                'isConfirmed': _('boolean')

            }
        )
        schema = Schema(schema=purchases)
        return schema.create(iterations=size)

    def generate_mobile_app_clickstream(self, purchases_list: list):
        # Generate sample data for generates purchases
        events_fullset = ['app_open', 'search_product', 'view_product_details', 'purchase', 'app_close']
        # TODO ADD events without purchases
        events_without_purchase = ['app_open', 'search_product', 'view_product_details', 'purchase', 'app_close']
        channels = ['Google Ads', 'Yandex Ads', 'Facebook Ads', 'Twitter Ads', 'VK Ads']
        campaign_ids = Random().randints(5, 100, 999)
        res = []

        for purchase in purchases_list:
            # print(purchase['purchaseId'] + ' | ' + purchase['purchaseTime'])
            purchase_date = datetime.datetime.strptime(purchase['purchaseTime'], "%Y-%m-%d %H:%M:%S")
            user_id = Cryptographic.uuid()
            app_open_date = purchase_date - datetime.timedelta(minutes=random.randint(10, 25),
                                                               seconds=random.randint(1, 59))
            search_date = app_open_date + datetime.timedelta(minutes=random.randint(5, 8),
                                                             seconds=random.randint(1, 59))
            view_date = search_date + datetime.timedelta(minutes=random.randint(1, 3),
                                                         seconds=random.randint(1, 59))
            app_close_date = purchase_date + datetime.timedelta(minutes=random.randint(1, 5))

            for type in events_fullset:
                mobile_event = {
                    'userId': user_id,
                    'eventId': Cryptographic.uuid(),
                    'eventType': type
                }
                if type == 'app_open':
                    mobile_event['eventTime'] = app_open_date.strftime("%Y-%m-%d %H:%M:%S")
                    attributes = {'campaign_id': random.choice(campaign_ids),
                                  'channel_id': random.choice(channels)}
                    mobile_event['attributes'] = attributes
                elif type == 'search_product':
                    mobile_event['eventTime'] = search_date.strftime("%Y-%m-%d %H:%M:%S")
                elif type == 'view_product_details':
                    mobile_event['eventTime'] = view_date.strftime("%Y-%m-%d %H:%M:%S")
                elif type == 'purchase':
                    mobile_event['eventTime'] = purchase_date.strftime("%Y-%m-%d %H:%M:%S")
                    attributes = {'purchase_id': purchase['purchaseId']}
                    mobile_event['attributes'] = attributes
                elif type == 'app_close':
                    mobile_event['eventTime'] = app_close_date.strftime("%Y-%m-%d %H:%M:%S")
                res.append(mobile_event)
        return res


def main():
    parser = argparse.ArgumentParser(prog='dlz_ingestion_service')

    parser.add_argument('--output_path', type=str, required=False, default='capstone-dataset/')
    # parser.add_argument('--size_lines_per_file', type=int, required=False, default=100)

    try:
        args = parser.parse_args()
        logging.info(args)
        ingestion_starter = DataGenerator(args.output_path)

        recreate_local_folder(os.path.join(args.output_path, 'user_purchases'))
        recreate_local_folder(os.path.join(args.output_path, 'mobile_app_clickstream'))
        for num in range(0, 50):
            purchases = ingestion_starter.generate_purchases(size=10000)
            purchase_name_csv = f'user_purchases_{num}.csv'
            with open(os.path.join(args.output_path, 'user_purchases', purchase_name_csv), 'w') as csvfile:
                writer = csv.DictWriter(csvfile, purchases[0].keys())
                writer.writeheader()
                writer.writerows(purchases)

            clickstream = ingestion_starter.generate_mobile_app_clickstream(purchases)
            clickstream_name_csv = f'mobile_app_clickstream_{num}.csv'
            with open(os.path.join(args.output_path, 'mobile_app_clickstream', clickstream_name_csv), 'w') as csvfile:
                writer = csv.DictWriter(csvfile, clickstream[0].keys())
                writer.writeheader()
                writer.writerows(clickstream)



    except Exception:
        logging.error(traceback.format_exc())
        sys.exit(99)


def recreate_local_folder(local_path):
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    main()
