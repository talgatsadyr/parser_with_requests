import psycopg2
import requests
from bs4 import BeautifulSoup
import time

headers = {"authority": 'yandex.ru',
           "method": 'GET',
           "path": '/ads/system/context.js',
           "scheme": 'https',
           "accept": '*/*',
           "accept-encoding": 'gzip, deflate, br',
           "accept-language": 'ru,en;q=0.9',
           "cache-control": 'no-cache',
           "user-agent": 'Mozilla/5.0 (Linux; Android 6.0; '
                         'Nexus 5 Build/MRA58N) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/102.0.5005.134 '
                         'Mobile Safari/537.36'
           }


def get_item_urls(file_name, url_of_site):
    response = requests.get(url=f"{url_of_site}", headers=headers)
    time.sleep(5)

    soup = BeautifulSoup(response.text, "lxml")
    items_divs = soup.find_all("div", class_="list-item list-label")
    urls = []
    for item in items_divs:
        item_url = item.find("a").get("href")
        urls.append(item_url)

    with open(f"{file_name}.txt", "a+") as file:
        for url in urls:
            file.write(f"https://www.mashina.kg{url}\n")

    return "Uspeshno"


category = {
    "legkovye": "https://www.mashina.kg/search/all/?page=",
    "kommercheskie": "https://www.mashina.kg/commercialsearch/all/?page=",
    "special-tech": "https://www.mashina.kg/specsearch/all/?page=",
    "zapchasti": "https://www.mashina.kg/partssearch/all/?page=",
    "uslugi": "https://www.mashina.kg/servicesearch/all/?page=",
    "moto": "https://www.mashina.kg/motosearch/all/?page=",
    "kuplyu": "https://www.mashina.kg/servicesearch/all/?service_category=34&page="
}
host = "127.0.0.1"
user = "postgres"
password = "qwerty"
db_name = "bot_users"

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)


def get_data(file_path, category_id):
    with open(file_path) as file:
        urls_list = [url.strip() for url in file.readlines()]

    for url in urls_list:
        response = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        ##### Price data
        try:
            item_price = soup.find("div", {"class": "head-left clr"}).find(
                'div', {"class": "price-types"}).find('h2').text.strip()
        except Exception as _ex:
            item_price = ''
        ######  Title name data
        try:
            item_name = soup.find("div", {"class": "head-left clr"}).find("h1").text.strip()
        except Exception as _ex:
            item_name = None
        ######## Phone number data
        try:
            item_phones = soup.find("a", class_="phone-num").get("href").strip().split(":")[-1]
        except Exception as _ex:
            item_phones = ''
        ######### Description data
        try:
            item_desc = soup.find("div", {"class": "main-info clr description"}).find("p").text.strip()

        except Exception as _ex:
            item_desc = None
        image_list = []
        ##### Image data
        try:
            item_img = soup.find('div', class_='fotorama').find_all('a', href=True)
            for i in item_img:
                image_list.append(f"{i.get('href')}")
        except Exception as _ex:
            image_list = ''
        ##### Datetime data
        created = ''
        try:
            item_date = soup.find("div", {"class": "right"}).find_all("span")
            count = 0
            for k in item_date:
                count += 1
                if count == 2:
                    created = k.text.strip()
        except Exception as _ex:
            created = ''
        ##### Datetime upgrade post
        upgraded = ''
        try:
            item_date = soup.find("div", {"class": "right"}).find_all("span")
            count = 0
            for k in item_date:
                count += 1
                if count == 4:
                    upgraded = k.text.strip()
        except Exception as _ex:
            upgraded = ''
        ###### Town data
        try:
            item_region = soup.find("p", {"class": 'town'}).find("a").get_text()
        except Exception as _ex:
            item_region = ''
        ##### Views
        try:
            item_views = soup.find("div", {"class": "right"}).find("span").text.strip()
        except Exception as _ex:
            item_views = 1
        # connect to exist database
        connection.autocommit = True
        # insert table
        if item_name is None:
            continue
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO Product(category_id,title,price,phone,description,created,upgraded,region,views)
                        VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s') Returning product_id""" %
                    (category_id,item_name,item_price,item_phones,f"{item_desc}",created,upgraded,item_region,item_views))
                product_id = cursor.fetchone()[0]
                print(product_id)
        except Exception:
            continue

        for k in image_list:
            with connection.cursor() as cursor:
                cursor.execute(f"""INSERT INTO images(product_id,img_urls)
                                    VALUES ({product_id},'{k}');""")
                print('[info] image table successfully inserted')


def main():

    ##### Get  urls of cars
    for key, value in category.items():
        for i in range(1, 10):
            get_item_urls(file_name=key, url_of_site=f"{value}{i}")
    connection.autocommit = True
    ##### Delete tables
    # with connection.cursor() as cursor:
    #     cursor.execute(
    #         """drop table images, product"""
    #     )
    #     print("[INFO]  table successfully deleted")
    #### Create table category
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE category(category_id integer PRIMARY KEY,title varchar(25));")
    #### Create table product
    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE product(
            product_id serial Primary Key,
            category_id integer References category(category_id),
            title varchar(250),
            price varchar(50),
            phone varchar(50),
            description text,
            created varchar(50),
            upgraded varchar(50),
            region varchar(50),
            views integer);""")
        print("[INFO] Product table created successfully")
    ##### Create table images
    with connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE images(
        img_id serial PRIMARY KEY,
        product_id integer REFERENCES product(product_id),
        img_urls varchar(250))""")
        print("[INFO] Image table successfully created")
    ##### Get items of cars
    category_id = 0
    for key in category:
        category_id += 1
        get_data(file_path=f"{key}.txt", category_id=category_id)
    ##### Insert table category
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO category(category_id,title)
                    VALUES ({category_id},'{key}');"""
            )
        print("[info] category table successfully inserted")
    # # #


if __name__ == "__main__":
    main()
