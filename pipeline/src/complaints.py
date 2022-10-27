from pymongo import MongoClient
# from src.mongoConnectors import getData
# from src.mongoConnectors import getAggregates


"""MongoDb Aggregate Pipeline"""
aggregate = {
        "$group": {
            "_id": {"date":"$createdAt", "category":"$productCategory"}, 
            "totalComplaintValue": {
                "$sum": "$complaintAmount"
            }, 
            "count": {
                "$sum": 1
            },
            "averageComplaintValue": {
                "$avg": "$complaintAmount"
            }
        }
    }

""" Aggregating Function """
def complaints(complaintsConn: dict, metricConnInfo: dict):

    def getData(url=metricConnInfo['url'], db=metricConnInfo['db'], coll='complaints'):
        """ Use this for the metric collections. """
        if True:
            try:
                metricColl = list(MongoClient(url)[db][coll].find())
                for data, newData in zip(metricColl, [dict(t) for t in set(tuple(sorted(d.items())) for d in [data['_id'] for data in metricColl])]):
                    data['_id'] == newData

                # return MongoClient(url)[db][coll], [dict(t) for t in set(tuple(sorted(d.items())) for d in metricColl_id)]
                # return list(MongoClient(url)[db][url].find())
                return MongoClient(url)[db][coll], metricColl

            except:
                print('Could not connect to mongoDb collection!')
    
    def getAggregates(url=complaintsConn['url'], db=complaintsConn['db'], coll='complaints', agg=aggregate):
        """ Use this to get aggregate from datalake. """
        if True:
            try:
                client = MongoClient(url)
                aggr = list(client[db][coll].aggregate([agg]))
                for data, newData in zip(aggr, [dict(t) for t in set(tuple(sorted(d.items())) for d in [data['_id'] for data in aggr])]):
                    data['_id'] == newData
                
                return aggr

                # return [dict(t) for t in set(tuple(sorted(d.items())) for d in list(MongoClient(url)[db][url].aggregate([agg])))]
                # return list(MongoClient(url)[db][url].aggregate([agg]))
            except:
                print('Getting aggreagte failed.')

    aggregatesList = getAggregates()
    metricConn, metricColl = getData()

    newEntry = [date for date in [d['_id'] for d in aggregatesList] if date not in [d['_id'] for d in metricColl]]
    changedEntry = [data['_id'] for data in metricColl if data not in aggregatesList]

    for data in aggregatesList:
        if data['_id']in newEntry:
            metricConn.insert_one(data)
        elif data['_id'] in changedEntry:
            query = { "_id": { "date": data['_id']["date"] } }
            metricConn.update_one(query, { "$set": {'complaintValue': data['complaintValue'],
                                                    'count': data['count'],
                                                    'averageComplaintValue': data['averageComplaintValue']} })
        else:
            None  



