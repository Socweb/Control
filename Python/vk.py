import vk_api
import datetime
import os
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from time import mktime, sleep
from sqlalchemy.sql.expression import func 
from dotenv import load_dotenv 

load_dotenv()

PHONE = os.getenv('PHONE')
PASSWORD = os.getenv('PASSWORD')
QUERIES_TUPLE = ('ищу репетитора', 'нужен репетитор' ,' посоветуйте репетитора')


vk_session = vk_api.VkApi(PHONE, PASSWORD)
vk_session.auth()

vk = vk_session.get_api()

# Получаем требуемые записи
def find_all_notes(queries_tuple: tuple, start_datetime: str) -> list:
    best_notes = []
    # Преобразуем строковое время типа '2022-01-01' в тип datetime
    start_datetime_correct_type = datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    # Получаем UNIX-время
    unix_start_datetime = mktime(start_datetime_correct_type.timetuple())
    for query in queries_tuple:
        all_notes = vk.newsfeed.search(
            q= query,
            extended = 1,
            count = 200,
            start_time = unix_start_datetime
        )
    # Поскольку запрос к API возвращает словарь, будем собирать нужные данные в список списков
    # [[owner_id: int, note_id: int, date: datetime, human: boolean, link: char, text: char],...]
        result_list = []
        for num, note in enumerate(all_notes['items']):
            # Больше 200 - ненужный пост
            if len(note['text']) < 200:
                result_list.append(
                    [
                        abs(note['owner_id']), note['id'],
                        datetime.datetime.utcfromtimestamp(note['date']).strftime('%Y-%m-%d %H:%M:%S'),
                        str(note['owner_id'])[0].isdigit(),
                        f'https://vk.com/wall-{abs(note["owner_id"])}_{note["id"]}',
                        note['text']
                    ]
                )
        best_notes.extend(result_list)

    return best_notes

i = 1
while i > 0:   
    engine = create_engine('postgresql+psycopg2://tutor:tutor@localhost:5432/tutor')

    metadata = MetaData(bind=engine, schema= 'public')
    MetaData.reflect(metadata)

    # Таблица с данными
    vk_table = Table('vk',  metadata, autoload = True , autoload_with = engine)
    # Техническая таблица для исключения уже записанных данных
    tech_table = Table('technical_data',  metadata, autoload = True , autoload_with = engine)

    # Максимальное время из уже записанного в БД
    max_datetime_db = ''

    # Если в БД уже есть записи
    if engine.execute('SELECT max(datetime) from vk;'):
        max_datetime_db = datetime.datetime.strftime(engine.execute('SELECT max(datetime) from vk;').fetchall()[0][0],'%Y-%m-%d %H:%M:%S')
    # Если их нет, то в качестве времени берём сутки назад
    else:
        max_datetime_db = datetime.datetime.strftime(datetime.datetime.utcnow() - datetime.timedelta(24) ,'%Y-%m-%d %H:%M:%S')
        
        
    result_list = find_all_notes(
        queries_tuple= QUERIES_TUPLE,
        start_datetime = max_datetime_db
    )


    result_list_of_dict_vk = [
        {
            'owner_id': x[0], 'note_id': x[1], 'datetime': x[2], 'human':x[3],
            'link':x[4], 'note':x[5]
        } for x in result_list
    ]
    result_dict_tech = [
        {
            'datetime': datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'),
            'social_network': 'vk'
        }
    ]

    insert_vk = pg_insert(vk_table).values(result_list_of_dict_vk)
    insert_tech = pg_insert(tech_table).values(result_dict_tech)
    engine.execute(insert_vk)
    engine.execute(insert_tech)
    engine.dispose()
    sleep(21600)
