import faust
import os
from dotenv import dotenv_values


from users import users
from complaints import complaints


config = dotenv_values(".env")


app = faust.App('metrics',broker="kafka://localhost:9092")

# MongoDB
conn = {
    'url': config['mongoDbURL'],
    'db': 'app',
    # 'coll': 'users'
}

metricConnInfo = {
    'url': config['mongoDbURL'],
    'db': 'metric',
    # 'coll': 'users'
}

userTopic = app.topic("my_kafka.users")
complaintTopic = app.topic("my_kafka.complaints")

userTable = app.Table('userTable_', default=int)
complaintTable = app.Table("complaintTable_",default=int)

@app.agent(userTopic)
async def count_hits(kafkaMessage):
    async for message in kafkaMessage:
        users(conn, metricConnInfo)

@app.agent(complaintTopic)
async def count_hits(kafkaMessage):
    async for message in kafkaMessage:
        complaints(conn, metricConnInfo)

if __name__ == '__main__':
    app.main()


