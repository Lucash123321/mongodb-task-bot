from motor.motor_asyncio import AsyncIOMotorClient
import datetime


class DataHandler:
    def __init__(self):
        client = AsyncIOMotorClient('mongodb://localhost:27017/')
        db = client['taskdb']
        self.collection = db['taskcollection']

    async def get_data(self, group_type, dt_from, dt_upto):

        dt_from = datetime.datetime.fromisoformat(dt_from)
        dt_upto = datetime.datetime.fromisoformat(dt_upto)

        if group_type == "month":
            group_data = {
                'year': {'$year': '$dt'},
                'month': {'$month': '$dt'},
                'day': {'$dayOfMonth': None}
            }

        elif group_type == "day":
            group_data = {
                'year': {'$year': '$dt'},
                'month': {'$month': '$dt'},
                'day': {'$dayOfMonth': '$dt'},
            }

        elif group_type == "hour":
            group_data = {

            }

        else:
            return []

        pipeline = [
            {
                '$densify': {
                    'field': "dt",
                    'range': {
                        'step': 1,
                        'unit': group_type,
                        'bounds': [dt_from, dt_upto + datetime.timedelta(milliseconds=1)]
                    }
                }
            },
            {
                '$match': {
                    'dt': {
                        '$gte': dt_from,
                        '$lte': dt_upto
                    }
                }
            },
            {
                '$group': {
                    '_id': group_data,
                    'total_sum': {'$sum': '$value'},
                    'date': {'$min': '$dt'}
                }
            },
            {
                '$sort': {'_id': 1}
            },
            {
                '$set': {
                    'total_sum': {'$cond': [{'$not': ['$total_sum']}, 0, '$total_sum']}
                }
            },
            {
                '$group': {
                    '_id': 0,
                    'sums': {
                        '$push': "$total_sum"
                    },
                    'intervals': {
                        '$push': "$date"
                    }
                }
            },
            {
                '$project': {
                    'dataset': '$sums', 'labels': '$intervals', '_id': 0
                }
            },
        ]

        selection = [obj async for obj in self.collection.aggregate(pipeline)]

        return selection
