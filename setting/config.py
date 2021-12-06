from flask import Flask, jsonify, request, Blueprint, make_response, url_for
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from flask_pymongo import PyMongo
from flask_cors import CORS
import requests, json, bson
import calendar
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from jsondiff import diff
import random
from hashlib import sha256
import base64
from datetime import datetime, timedelta, date
from time import gmtime, strftime, localtime
from flask_jwt_extended import JWTManager, create_access_token
import pyodbc
import pymysql

apps = Flask(__name__)

jwt = JWTManager(apps)

