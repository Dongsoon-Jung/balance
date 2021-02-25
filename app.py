# -*- coding: utf-8 -*-
from chalice import Chalice, NotFoundError
import pymysql
import logging
import json

app = Chalice(app_name='balance')
rds_host = "rds-candipay.cxjgh9mqa3mo.ap-northeast-2.rds.amazonaws.com"
username = "balance_app"
password = "1"
db_name = "balance"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OBJECTS = {
}

TX_SIGN = {
    "normal": {"use": "-", "charge": "+", "trans_send": "-", "trans_recv": "+"},
    "cancel": {"use": "+", "charge": "-"},
    "nw-cancel": {"use": "+", "charge": "-"}
}


@app.route('/balance/charge', methods=['POST', 'PUT'])
def charge():
    request_body = app.current_request.json_body

    user_id = request_body['user_id']
    tx_type = request_body['tx_type']
    tx_category = request_body['tx_category']
    money_type = request_body['money_type']
    money_balance_amount = request_body['money_balance_amount']
    balance_transaction_log_id = None

    try:
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=db_name, connect_timeout=3)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    try:

        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("insert into balance_transaction_log values (null, %s, now(), %s, now(), now())"
                           , (user_id, json.dumps(request_body)))

            balance_transaction_log_id = cursor.lastrowid

            find_user_sql = '''
                    select   a.id,
                             a.user_id
                      from   balance a
                     where   a.user_id = %s
            '''

            insert_balance_sql = '''
                insert into balance values (null, %s, %s, now(), now())
                on duplicate key update 
                balance_amount = balance_amount + %s                
            '''

            insert_balance_detail_sql = '''
                insert into balance_detail
                values (null, %s, %s, %s, %s, now(), now()) 
                on duplicate key update 
                money_balance_amount = money_balance_amount + %s
            '''

            insert_balance_transaction_sql = '''
                insert into balance_transaction values (null, %s, %s, %s, %s, %s, %s, now(), now())
            '''

            cursor.execute(insert_balance_sql, (user_id, money_balance_amount, money_balance_amount))
            inserted_balance_id = cursor.lastrowid
            cursor.execute(insert_balance_detail_sql, (user_id, money_type,
                                                       money_balance_amount, inserted_balance_id,
                                                       money_balance_amount))
            insert_history_data = (tx_category, tx_type, money_type,
                                   money_balance_amount, user_id, balance_transaction_log_id)

            cursor.execute(insert_balance_transaction_sql, insert_history_data)

    finally:
        conn.commit()
        conn.close()

    return {'status': 'SUCCESS', 'transaction_id': balance_transaction_log_id}


@app.route('/balance', methods=['POST', 'PUT'])
def balance():
    request_body = app.current_request.json_body

    user_id = request_body['user_id']
    balance_detail = request_body['balance_detail']

    balance_total_amount = 0
    balance_transaction_log_id = None

    try:
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=db_name, connect_timeout=3)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    for item in balance_detail:
        balance_sign = get_sign(item['tx_category'], item['tx_type'])
        if balance_sign is None:
            raise ValueError("Input Error")
        item['balance_sign'] = balance_sign
        if balance_sign == "+":
            balance_total_amount = balance_total_amount + item['money_balance_amount']
        elif balance_sign == "-":
            balance_total_amount = balance_total_amount - item['money_balance_amount']
        else:
            raise ValueError("Input Error")

    try:

        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("insert into balance_transaction_log values (null, %s, now(), %s, now(), now())"
                           , (user_id, json.dumps(request_body)))

            balance_transaction_log_id = cursor.lastrowid

            update_balance_sql = '''
                update balance set balance_amount = balance_amount + %s where user_id = %s
            '''

            insert_balance_transaction_sql = '''
                insert into balance_transaction values (null, %s, %s, %s, %s, %s, %s, now(), now())
            '''

            insert_balance_detail_sql = '''
                insert into balance_detail
                values (null, %s, %s, %s, %s, now(), now()) 
                on duplicate key update 
                money_balance_amount = money_balance_amount + %s
            '''

            cursor.execute(update_balance_sql, (balance_total_amount, user_id))
            inserted_balance_id = cursor.lastrowid

            for row in balance_detail:
                tx_category = row['tx_category']
                tx_type = row['tx_type']
                balance_sign = row['balance_sign']
                money_type = row['money_type']
                money_balance_amount = row['money_balance_amount'] * (1 if balance_sign == "+" else -1)

                cursor.execute(insert_balance_detail_sql, (user_id, money_type,
                                                           money_balance_amount, inserted_balance_id,
                                                           money_balance_amount))

                insert_history_data = (tx_category, tx_type, money_type, money_balance_amount,
                                       user_id, balance_transaction_log_id)
                cursor.execute(insert_balance_transaction_sql, insert_history_data)

    finally:
        conn.commit()
        conn.close()

    return {'status': 'SUCCESS', 'transaction_id': balance_transaction_log_id}


@app.route('/balance', methods=['GET'])
def balance():
    try:
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=db_name, connect_timeout=3)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    data = dict()
    sub_record = list()
    param_user_id = app.current_request.query_params['user_id']
    print(param_user_id)

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = '''
                    select   a.user_id,
                             a.balance_amount,
                             b.money_type,
                             b.money_balance_amount
                      from   balance a
                      join   balance_detail b
                        on   a.id = b.balance_id
                     where   a.user_id = %s
            '''
            cursor.execute(sql, param_user_id)

            result = cursor.fetchall()
            for idx, row in enumerate(result):
                print(row)
                if idx == 0:
                    data['balance_amount'] = row['balance_amount']
                sub_record.append({'money_type': row['money_type'], 'money_balance_amount': row['money_balance_amount']})
            data['balance_detail'] = sub_record
            print(data)

    finally:
        conn.close()

    return data


@app.route('/balance/charge/limit', methods=['GET'])
def get_charge_limit():
    return {'min_charge': 50000, 'max_charge': 1000000, 'charge_unit': 10000}


def get_sign(tx_category, tx_type):
    return TX_SIGN.get(tx_category).get(tx_type)

