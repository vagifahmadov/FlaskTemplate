from setting.config import request, Blueprint, jsonify, make_response, jwt, JWTManager, create_access_token, url_for, sha256
from setting.db import mongo, apps, mssql
from objects.defs import convert_table_data_dict, query_maker, run_sql_with_transaction, select_query

select_data = Blueprint('selectData', __name__)


@select_data.route('/selectData', methods=['GET'])
def select_data_func():
    # vars
    id_b = request.args.get("id")
    if id_b is not None:
        statement = "id"
        statement_value = id_b
        condition = "="
    else:
        statement = ""
        statement_value = ""
        condition = ""

    output = select_query(table_name="a1", type_db="mysql", statement=statement, statement_compare_value=statement_value, condition=condition)
    return jsonify({'data': output})


insert_data = Blueprint('insertData', __name__)


@insert_data.route('/insertData', methods=['POST'])
def insert_data_func():
    data_request = request.json
    key_list = list(data_request.keys())
    query_schema = []
    list(map(lambda qs: query_schema.append({qs: data_request[qs]}), key_list))
    query = query_maker(table="a1", values_and_rows=query_schema, type_query="insert")
    result = run_sql_with_transaction(sql_query_list=[query], db_type="mssql")
    output = result
    return jsonify({'data': output})


update_data = Blueprint('updateData', __name__)


@update_data.route('/updateData', methods=['PUT'])
def update_data_func():
    data_request = request.json
    key_list = list(data_request.keys())
    statement_key = "id"
    statement_type = "="
    query_schema = []
    list(map(lambda qs: query_schema.append({qs: data_request[qs]}), key_list))
    query = query_maker(table="a1", values_and_rows=query_schema, type_query="update", statement=statement_key, statement_type=statement_type, compare_value=data_request[statement_key])
    result = run_sql_with_transaction(sql_query_list=[query], db_type="mysql")
    output = result
    return jsonify({'data': output})

