from mongoConnectors import getData
from mongoConnectors import getAggregates


def users(usersConn: dict, metricConnInfo: dict):

    aggregate = {
            '$group': {
                '_id': '$date', 
                'count': {
                    '$sum': 1
                }
            }
        }

    def func_(aggr):
        for data in aggr:
            data['date']=data.pop('_id')

    aggregatesList = getAggregates(usersConn['url'], usersConn['db'], 'users', aggregate)
    metricConn, metricColl = getData(metricConnInfo['url'], metricConnInfo['db'], 'users', func_)


    newEntry = [date for date in [d['date'] for d in aggregatesList] if date not in [d['date'] for d in metricColl]]
    changedEntry = [data['date'] for data in metricColl if data not in aggregatesList]

    print(aggregatesList)


    for data in aggregatesList:
        if data['date'] in newEntry:
            metricConn.insert_one({'date': data['date'], 'count': data['count']})
        elif data['date'] in changedEntry:
            metricConn.update_one({'date': data['date']}, { "$set": { 'count': data['count'] } })
        else:
            None