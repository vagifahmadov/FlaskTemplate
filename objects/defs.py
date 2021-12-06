from setting.config import *
from setting.db import mongo, mssql, con_mssql, mysql, con_mysql
import random


def return_output(data, message, status):
    return {"message": message, "data": data, "status": status}


def validate(request_data, validate_list):
    validation_list = []
    request_keys = []
    result = []
    valid_sub_key = []
    main_keys_only = []
    status_code = 200
    for key, value in validate_list.items():
        validation_list.append(key)  # to load given validation key

    for key, value in request_data.items():
        if key not in validation_list:
            main_keys_only.append(key)
        request_keys.append(key)  # to load request keys
        if type(value) is dict:
            for k, v in value.items():
                if k not in validation_list:
                    valid_sub_key.append(key + "." + k)
                request_keys.append(k)  # to load request sub keys
        else:
            if value is None or not value or value == " ":
                status_code = validate_list[key]

    keys_difference = list(set(validation_list) - set(request_keys))  # find difference

    if keys_difference:
        # if keys is invalid
        status_code = 1005
        if valid_sub_key:
            if len(valid_sub_key) == len(keys_difference) and len(valid_sub_key) == 1:
                output = {"request": valid_sub_key[0],
                          'actually': str(valid_sub_key[0]).split(".")[0] + "." + keys_difference[0]}
            elif len(valid_sub_key) == len(keys_difference):
                output = {"request": valid_sub_key, 'actuallySubKeys': keys_difference}
            else:
                output = {"request": {"mainKeys": main_keys_only, "subKeys": valid_sub_key},
                          "actually": keys_difference}
                # output=keys_difference+valid_sub_key
        else:
            if len(main_keys_only) > 0:
                if len(keys_difference) == 1:
                    print(main_keys_only)
                    output = {"request": main_keys_only[0], "actually": keys_difference[0]}
                else:
                    output = {"request": {"mainKeys": main_keys_only}, "actually": keys_difference}
            else:
                if len(keys_difference) == 1:
                    actually = keys_difference[0]
                else:
                    actually = keys_difference
                output = {"Request": "No such key", "actually": actually}
    else:
        # if key valid and no values
        for k_v, v_v in request_data.items():
            if type(v_v) is dict:
                for k, v in v_v.items():
                    if type(v_v[k]) is not bool and not v_v[k]:  # if value is not empty or not bool
                        result.append(k_v + "." + k)
                        status_code = validate_list[k]
                        break
            if not request_data[k_v]:
                result.append(k_v)
                status_code = validate_list[k_v]
        output = result

    return {"code": status_code, 'data': output}


def error_codes_manage(process, code=None, return_code=None, message=None, update=None, new_code=None, new_return_code=None):
    error_base = mongo.db.errorBase
    return_codes = mongo.db.return_codes

    error_schema = {
        "code": code,
        "message": message
    }

    return_schema = {
        "code": code,
        "statusCode": return_code
    }

    error_messages = [
        "Error has already in base",  # 0
        "Error not inserted",  # 1
        "Error not in base",  # 2
        "Error can not removed",  # 3
        "Error can not inserted",  # 4
        "Process not registered",  # 5
        "Success",  # 6
        "Some variables are None",  # 7
        "Error can not update",  # 8
        "Update process not registered",  # 9
    ]

    if process == "delete":
        if code is not None or return_codes is not None:
            if code is not None:
                table = error_base
                process_code = code
                key = "code"
            elif return_code is not None:
                table = return_codes
                process_code = return_code
                key = "statusCode"
            else:
                return error_messages[7]

            find_error = table.find_one({key: process_code})
            if find_error is not None:
                if key == "code":
                    remove_error = table.remove({key: process_code})
                    if remove_error is not None:
                        remove_error = return_codes.remove({key: process_code})
                        if remove_error is None:
                            return error_messages[3]
                        else:
                            return error_messages[6]
                    else:
                        return error_messages[3]
                elif key == "statusCode":
                    find_code = find_error['code']
                    remove_error = table.remove({key: process_code})
                    if remove_error is None:
                        return error_messages[3]
                    else:
                        remove_error = error_base.remove({"code": find_code})
                        if remove_error is None:
                            return error_messages[3]
                        else:
                            return error_messages[6]
                else:
                    return error_messages[3]
            else:
                return error_messages[2]
        else:
            return error_messages[7]
    elif process == "insert":
        if code is not None and return_code is not None and message is not None:
            check_code = error_base.find_one({"code": code})
            if check_code is None:
                result_error = error_base.insert(error_schema)
                if result_error is not None:
                    result_return = return_codes.insert(return_schema)
                    if result_return is not None:
                        return error_messages[6]
                    else:
                        error_base.remove(error_schema)
                        return error_messages[1]
                else:
                    return error_messages[1]
            else:
                return error_messages[0]
        else:
            return error_messages[7]
    elif process == "update":
        if update == "code":
            update_search_schema = {update: code}
            update_schema = {update: new_code}
            if code is not None and new_code is not None:
                find_update_code = error_base.find_one(update_search_schema)
                if find_update_code is not None:
                    update_code = error_base.update_one(update_search_schema, {'$set': update_schema})
                    if update_code is not None:
                        update_code = return_codes.update_one(update_search_schema, {'$set': update_schema})
                        if update_code is not None:
                            return error_messages[6]
                        else:
                            return error_messages[8]
                    else:
                        return error_messages[8]
                else:
                    return error_messages[2]
            else:
                return error_messages[8]
        elif update == "statusCode":
            update_search_schema = {update: return_code, "code": code}
            update_schema = {update: new_return_code}
            if new_return_code is not None and return_code is not None and code is not None:
                find_return_code = return_codes.find_one(update_search_schema)
                if find_return_code is not None:
                    update_code = return_codes.update_one(update_search_schema, {'$set': update_schema})
                    if update_code is not None:
                        print("OK")
                        return error_messages[6]
                    else:
                        return error_messages[8]
                else:
                    return error_messages[2]
            else:
                return error_messages[8]
        elif update == "message":
            if code is not None:
                update_search_schema = {"code": code}
                update_schema = {update: message}
                find_message = error_base.find_one(update_search_schema)
                if find_message is not None:
                    update_code = error_base.update_one(update_search_schema, {'$set': update_schema})
                    if update_code is not None:
                        return error_messages[6]
                    else:
                        return error_messages[8]
                else:
                    return error_messages[2]
            else:
                return error_messages[8]
        else:
            return error_messages[9]
    elif process == "get":
        status_code_list = []
        find_status_codes = list(error_base.find({}, {"_id": False}))
        for status_code_item in find_status_codes:
            result_return_code = return_codes.find_one({"code": status_code_item['code']}, {"_id": False, "code": False})
            if result_return_code is not None:
                result_return_code = result_return_code['statusCode'] if 'statusCode' in result_return_code else None
            status_code_list.append({
                "code": status_code_item['code'],
                "message": status_code_item["message"],
                "returnCode": result_return_code
            })
        return status_code_list
    else:
        return error_messages[5]


def current_full_str_date():
    return strftime("%d.%m.%Y %H:%M:%S", localtime())


def current_str_date():
    today = date.today()
    return today.strftime("%d.%m.%Y")


def current_str_time():
    format_time = "%H:%M"
    return datetime.now().time().strftime(format_time)


def current_full_obj_date():
    full_format_date = "%d.%m.%Y %H:%M:%S"
    return datetime.today().strptime(current_full_str_date(), full_format_date)


def convert_table_data_dict(table, type_db: str, mysql_query=None):
    if type_db == "mssql" and mysql_query is None:
        table_headers = table.description
        table_headers = list(map(lambda h: h[0], table_headers))
    elif type_db == "mysql" and mysql_query is not None:
        q = "desc "+str(table)
        mysql.execute(q)
        table_headers = list(map(lambda h: h[0], mysql.fetchall()))
        mysql.execute(mysql_query)
        table = mysql
    else:
        return None
    table_data_helper = table.fetchall()
    table_data = list(map(lambda d: dict(zip(table_headers, d)), table_data_helper))
    # process
    output = None if len(table_data) == 0 else table_data
    return output


def insert_query_helper(values_and_rows: list, type_element: str):
    """
    This function is helps to make insert query
    :param values_and_rows:
    :param type_element:
    :return:
    """
    query_elements = []
    list(map(lambda u: u.update({str(next(iter(u.keys()))): '\'' + str(next(iter(u.values()))) + '\''}) if type(u[str(next(iter(u.keys())))]) is str else None, values_and_rows))
    if type_element == "keys":
        list(map(lambda ql: query_elements.append(str(next(iter(ql.keys()))) + ", "), values_and_rows))
    elif type_element == "values":
        list(map(lambda ql: query_elements.append(str(next(iter(ql.values()))) + ", "), values_and_rows))
        query_elements = list(map(lambda q: q.replace("''", "'"), query_elements))
    else:
        return None
    query_elements[-1] = query_elements[-1][:-2]
    result = "".join(query_elements)
    return result


def update_query_helper(query: str, values_and_rows: list, statement: str):
    """
    This function is helps to make insert query
    :param statement:
    :param values_and_rows:
    :param query:
    :return:
    """
    query_elements = []
    helper_str_add = ""
    values_and_rows = list(filter(lambda f: str(next(iter(f.keys()))) != statement, values_and_rows))
    list(map(lambda u: u.update({str(next(iter(u.keys()))): "'"+str(next(iter(u.values())))+"'"}) if type(u[str(next(iter(u.keys())))]) is str else None, values_and_rows))
    list(map(lambda q: query_elements.append(str(next(iter(q.keys()))) + "=" + str(next(iter(q.values()))) + ", "), values_and_rows))
    helper_str_add = helper_str_add.join(query_elements)
    query += helper_str_add
    result = query[:-2]
    return result


def query_maker(table: str, values_and_rows: list, type_query: str, statement: str = '', statement_type: str = '', compare_value=None):
    """
    This method modeling the query
    :param compare_value:
    :param statement_type:
    :param statement: <condition> (it works if type_query is update)
    :param type_query: <update> / <insert>
    :param table: <table name>
    :param values_and_rows: {<row>: <value>}
    :return: full sql query
    """
    result = None
    query = ""
    type_query = type_query.lower()
    if type_query in ["update", "insert"]:
        if type_query == "update" and len(statement) > 0 and len(statement_type) > 0 and compare_value is not None:
            query = query + "UPDATE " + str(table) + " SET "
            query = update_query_helper(query=query, values_and_rows=values_and_rows, statement=statement)
            result = query + " WHERE " + str(statement) + statement_type + str(compare_value) + ";"
        elif type_query == "insert":
            query = query + "INSERT INTO " + str(table) + " ("
            helper = insert_query_helper(values_and_rows=values_and_rows, type_element="keys")
            query = None if helper is None else query + helper + ") VALUES ("
            if query is not None:
                helper = insert_query_helper(values_and_rows=values_and_rows, type_element="values")
                query = None if helper is None else query + helper + ");"
            result = query
    return result


def run_sql_with_transaction(sql_query_list: list, db_type: str):
    """
    This function is working with transaction. Using insert and update process only.
    :param sql_query_list:
    :param db_type:
    :return:
    """
    error = True
    if db_type == "mssql":
        con_type_db = con_mssql
        type_db = mssql
        con_error = pyodbc.Error
    elif db_type == "mysql":
        con_type_db = con_mysql
        type_db = mysql
        con_error = pymysql.Error
    else:
        output = {'error': error, 'status': 'Incorrect value inserted'}
        return output
    try:
        error = False
        con_type_db.autocommit = False
        list(map(lambda sql_query: type_db.execute(sql_query), sql_query_list))
        con_type_db.commit()
        output = {'error': error, 'status': 'Success'}
    except KeyError:
        result = None
        con_type_db.rollback()
        output = {'error': result, 'status': 'Connection lost'}

    return output


def select_query(table_name: str, type_db: str, condition: str = "", statement: str = "", statement_compare_value: str = ""):
    query = "select * from {table_name} {ids}"
    mysql_table = None
    if statement != "" and statement_compare_value != "" and condition != "":
        query = query.format(ids="where "+str(statement) + condition + statement_compare_value, table_name=table_name)
    else:
        query = query.replace(' {ids}', '')
        query = query.format(table_name=table_name)
    if type_db == "mysql":
        mysql_table = query
        tab = table_name
    elif type_db == "mssql":
        con_type = mssql
        tab = con_type.execute(query)
    else:
        return None
    output = convert_table_data_dict(table=tab, type_db=type_db, mysql_query=mysql_table)
    return output
