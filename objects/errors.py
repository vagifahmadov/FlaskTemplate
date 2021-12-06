from setting.db import mongo


def message_output(code):

    error_base = mongo.db.errorBase
    output = "Error code not in base"
    list_error = error_base.find_one({'code': code})
    if list_error:
        output = list_error['message']
    return output
