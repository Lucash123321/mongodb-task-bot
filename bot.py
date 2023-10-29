from aiogram import Bot, types, Dispatcher, executor
from dotenv import load_dotenv
import os
import json
from datahandler import DataHandler

load_dotenv()
TOKEN = os.getenv("TOKEN")
bot = Bot(token=TOKEN)

dispatcher = Dispatcher(bot)
data_handler = DataHandler()


@dispatcher.message_handler()
async def message(msg: types.Message):
    json_request = json.loads(msg.text)
    response = await data_handler.get_data(json_request['group_type'],
                                           json_request['dt_from'],
                                           json_request['dt_upto'])

    for index in range(len(response[0]['labels'])):
        response[0]['labels'][index] = response[0]['labels'][index].isoformat()

    json_response = json.dumps(*response)
    await bot.send_message(msg.from_user.id, json_response)


if __name__ == '__main__':
    executor.start_polling(dispatcher)
