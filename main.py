from telegram.ext import Updater, CommandHandler
import logging
import requests
import os
import psycopg2
from bs4 import BeautifulSoup

mainpage = "https://www.sahibinden.com"

website = "https://www.sahibinden.com/" \
          "satilik-arsa?address_town=870&address_town=872&address_town=874" \
          "&address_town=875&address_town=1083&address_town=1082&a507" \
          "_min=1500&address_city=67"
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


def crawling(website, link_class):
    req = requests.get(website, headers=headers)
    content = BeautifulSoup(req.content, 'html.parser')

    class_members = content.find_all(class_=link_class)

    for i in class_members:
        iutf8 = i.get("title").encode("utf-8")

        ad_titles.append(iutf8)
        ad_links.append(mainpage + i.get("href"))

    listzip = list(zip(ad_titles, ad_links))
    return listzip


links = crawling(website, link_class)


def check(update, context):
    print("checking...")
    try:
        DATABASE_URL = os.environ['DATABASE_URL']
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        search_db = conn.cursor()
        search_db.execute('CREATE TABLE IF NOT EXISTS farms (id SERIAL, farm TEXT NOT NULL)')
    except:
        pass

    for item in links:
        title = item[0].decode('utf-8')
        link = item[1]
        message = title + " " + link
        farm_exists = search_db.execute('SELECT farm FROM farms WHERE farm = %s', [title])
        if not farm_exists:
            context.bot.send_message(chat_id=update.effective_chat.id, text=message)
            search_db.execute('INSERT INTO farms (farm) VALUES (%s);', [title])
            conn.commit()
        else:
            continue

    # end SQL connection
    search_db.close()


check_handler = CommandHandler('check', check)
dispatcher.add_handler(check_handler)
updater.start_polling()

