from telegram.ext import Updater, CommandHandler
import logging
import requests
import psycopg2
import os
from bs4 import BeautifulSoup

mainpage = "https://www.sahibinden.com"

website = "https://www.sahibinden.com/satilik-arsa?pagingSize=50&a507_min=1500&sorting=date_desc&address_town=870&address_town=872&address_town=874&address_town=875&address_town=1083&address_town=1082&price_max=400000&address_city=67"
headers = {
    'Host': 'www.sahibinden.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'tr-TR,tr;q=0.9,en-TR;q=0.8,en;q=0.7,en-US;q=0.6',
    'Connection': 'keep-alive',
}

link_class = "classifiedTitle"

updater = Updater(token=os.environ['TG_TOKEN'], use_context=True)
dispatcher = updater.dispatcher

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


ad_titles = []
ad_links = []


def crawling():
    req = requests.get(website, headers=headers)
    content = BeautifulSoup(req.content, 'html.parser')

    class_members = content.find_all(class_=link_class)

    for i in class_members:
        iutf8 = i.get("title").encode("utf-8")

        ad_titles.append(iutf8)
        ad_links.append(mainpage + i.get("href"))

    listzip = list(zip(ad_titles, ad_links))
    return listzip


links = crawling()


def connect_db():
    conn = None
    try:
        DATABASE_URL = os.environ['DATABASE_URL']
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        return cur
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
    finally:
        conn.commit()


def send_message(context, update, message_to_send):
    context.bot.send_message(chat_id=update.effective_chat.id, text=message_to_send)


def insert_items(farm_list, context, update):
    sql_exp = """INSERT INTO farms(ad_exp, ad_link) VALUES(%s,%s);"""
    sql_select = """SELECT EXISTS(SELECT 1 FROM farms WHERE ad_link = %s)"""

    conn = None
    try:
        DATABASE_URL = os.environ['DATABASE_URL']
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        for exp, link in farm_list:
            cur.execute(sql_select, (link,))
            sql_res = cur.fetchone()

            if not sql_res == (True,):
                cur.execute(sql_exp, (exp, link,))
                message_to_send = exp + " " + link
                send_message(context, update, message_to_send)
            else:
                message_to_send = "Yeni bir ilan bulunamadı."

        send_message(context, update, message_to_send)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS farms (
            farm_id SERIAL PRIMARY KEY,
            ad_exp VARCHAR(255) NOT NULL,
            ad_link VARCHAR(255) NOT NULL
        )
        """)
    conn = None
    try:
        cur = connect_db()
        cur.execute(commands)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("db_create:", error)
    finally:
        if conn is not None:
            conn.close()


def start(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Yeni ilanları kontrol etmek için /check yazın. \n"
                                  "Kontrol edilen ilan özellikleri: \n"
                                  "İl: Zonguldak\n"
                                  "İlçeler: Çaycuma, Ereğli, Gökçebey, Kilimli, Kozlu, Merkez\n"
                                  "Fiyat: 400.000 ve daha düşük\n"
                                  "Metrekare: 1500 m2 ve üzeri"
                            )


def check(update, context):
    print("checking...")
    try:
        cur = connect_db()
        titles = []

        for item in links:
            title = item[0].decode('utf-8').replace("'", "")
            link = item[1]
            titles.append((title, link))

        print("create start")
        create_tables()
        print("create done")
        insert_items(titles, context, update)

    except Exception as err:
        print("Something went wrong: ", err)

    finally:
        # end SQL connection
        if cur:
            cur.close()


start_handler = CommandHandler('start', start)
check_handler = CommandHandler('check', check)
dispatcher.add_handler(start_handler)
dispatcher.add_handler(check_handler)
updater.start_polling()

