from urllib.parse import parse_qsl
from pymongo import MongoClient
from sqlalchemy import func

def getData(url: str, db: str, coll: str):
    """ Use this for the metric collections. """
    if True:
        try:
            return MongoClient(url)[db][coll], [dict(t) for t in set(tuple(sorted(d.items())) for d in list(MongoClient(url)[db][coll].find()))]
            # return list(MongoClient(url)[db][url].find())
        except:
            print('Could not connect to mongoDb collection!')
            

def getAggregates(url: str, db: str, coll: str, agg: dict, func_):
    """ Use this to get aggregate from datalake. """
    if True:
        try:
            client = MongoClient(url)
            aggr = list(client['app']['users'].aggregate([agg]))
            
            func_(aggr)

            aggr = [dict(t) for t in set(tuple(sorted(d.items())) for d in aggr)]
            # helloList = [dict(t) for t in set(tuple(sorted(d.items())) for d in helloList)]
            return aggr

            # return [dict(t) for t in set(tuple(sorted(d.items())) for d in list(MongoClient(url)[db][url].aggregate([agg])))]
            # return list(MongoClient(url)[db][url].aggregate([agg]))
        except:
            print('Getting aggreagte failed.')

