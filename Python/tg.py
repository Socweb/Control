import os
import telebot
import datetime
from sqlalchemy import create_engine, Table, MetaData, select
import csv
from dotenv import load_dotenv 

load_dotenv()

TOKEN = os.getenv('TOKEN_TG')
PG_LOGIN = os.getenv('PG_LOGIN')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_DB = os.getenv('PG_DB')

bot = telebot.TeleBot(TOKEN, parse_mode=None)

def get_data(period):
    engine = create_engine(f'postgresql+psycopg2://{PG_LOGIN}:{PG_PASSWORD}@localhost:5432/{PG_DB}')

    metadata = MetaData(bind=engine, schema= 'public')
    MetaData.reflect(metadata)

    vk = Table('vk',  metadata, autoload = True , autoload_with = engine)

    start_date  = datetime.datetime.now() - datetime.timedelta(period)
    # Выбираем колонки
    vk_unprocessed = select([vk]).where(vk.c.datetime >= start_date )
    # Формируем забрас на получение
    res = engine.execute(vk_unprocessed).fetchall()

    # Передаём строки в список
    vk_list =[x for x in res]

    # Записываем в csv-файд
    with open('vk.csv', 'w', newline='') as csvfile:
        fieldnames = ['id', 'owner_id', 'note_id', 'datetime', 'human', 'link', 'note']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in vk_list:
            row_data = {
                "id": row[0],
                "owner_id": row[1],
                "note_id": row[2],
                "datetime": row[3],
                "human": row[4],
                "link": row[5],
                "note": row[6],

            }
            writer.writerow(row_data)
    engine.dispose()
    return 0

def query_in_db(message, period):
    if message.text.lower() in ('да', 'д', 'y', 'yes'):
        get_data(period)
        doc = open('vk.csv', 'r')
        bot.send_document(message.chat.id, doc)
        return True
    elif message.text.lower() in ('нет', 'н', 'n', 'no'):
        bot.send_message(message.chat.id, 'Спасибо за то, что воспользовались ботом! Ждём Вашего возвращения!')
    else:
        bot.send_message(message.chat.id, 'Отправлен недействующий вариант ответа. Переход в основное меню')
        bot.register_next_step_handler(message, send_start)
        return False
   
@bot.message_handler(commands= ['start'])
def send_start(message):
    bot.reply_to(message, "Выберите нужную команду: \n/show - показать варианты за определённый период \n/cancel - отмена")
    
@bot.message_handler(commands= ['show'])
def send_show(message):
    bot.reply_to(message, "Выберите период: \n/1w - за последнюю неделю \n/3d - за последние 3 дня \n/1d -за последний день")    
    
@bot.message_handler(commands= ['1w'])
def send_1w(message):
    bot.reply_to(message, "Подтверждаете запрос на получение данных? (Да/Нет)")
    bot.register_next_step_handler(message, query_in_db, 7)

@bot.message_handler(commands= ['3d'])
def send_3d(message):
    bot.reply_to(message, "Подтверждаете запрос на получение данных? (Да/Нет)")
    bot.register_next_step_handler(message, query_in_db, 3)

@bot.message_handler(commands= ['1d'])
def send_1d(message):
    bot.send_message(message.chat.id, "Подтверждаете запрос на получение данных? (Да/Нет)")
    bot.register_next_step_handler(message, query_in_db, 1)
    
@bot.message_handler(func=lambda m: True)
def echo_all(message):
    bot.reply_to(message, "Выберите нужную команду: \n/show - показать варианты за определённый период \n/cancel - отмена")

bot.polling()