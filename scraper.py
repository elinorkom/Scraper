import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
import argparse
import sys

# Scrape function -  Scrapping data from flickr into a SQL database
def scrape(keyword, size, api_key):
    lst_photo_ids = []
    results_dict = {}
    lst_urls = []

    flickr_base_url = "https://www.flickr.com/services/rest"
    get_photos_method = "?method=flickr.photos.search"
    get_photos_url = flickr_base_url + get_photos_method
    params = {
        'api_key': api_key,
        'text': keyword,
        'per_page': size
    }

    try:
        response = requests.get(get_photos_url, params)
    except requests.exceptions.HTTPError as errh:
        print("Http Error: ", errh)
        sys.exit()
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting: ", errc)
        sys.exit()
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
        sys.exit()

    parsed_response = BeautifulSoup(response.text, 'xml')
    photos = parsed_response.findAll('photo')

    for photo in photos:
        photoID = photo.get('id')
        lst_photo_ids.append(photoID)

    get_info_method = "?method=flickr.photos.getInfo"
    get_info_url = flickr_base_url + get_info_method

    for photo_id in lst_photo_ids:
        params_info = {
            'api_key': api_key,
            'photo_id': photo_id
        }

        try:
            response_info = requests.get(get_info_url, params_info)
        except requests.exceptions.HTTPError as errh:
            print("Http Error: ", errh)
            lst_photo_ids.remove(photo_id)
            continue
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting: ", errc)
            lst_photo_ids.remove(photo_id)
            continue
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)
            lst_photo_ids.remove(photo_id)
            continue

        soup_info_parsed = BeautifulSoup(response_info.text, 'xml')
        photo_url = soup_info_parsed.find('photo').find('url').get_text()
        if photo_url is None or photo_url == "":
            lst_photo_ids.remove(photo_id)
            continue
        lst_urls.append(photo_url)
        results_dict[photo_id] = photo_url

    if len(results_dict) == 0:
        print('No results were found')
        sys.exit()

    time = datetime.now()

    cnx = mysql.connector.connect(user='root', password='Password', port=13306, host='127.0.0.1', database='scraper')
    cursor = cnx.cursor()  # The purpose of cursor is controlling the table row management

    insert_image_query = ("INSERT INTO images"
                          "(keyword, imageUrl, scrapeTime)"
                          "VALUES (%s, %s, %s)")

    for key, value in results_dict.items():
        image_values = (keyword, value, time)
        cursor.execute(insert_image_query, image_values)

    cnx.commit()
    cursor.close()
    cnx.close()
    print("number of results inserted: " + str(len(results_dict)))

# Query function - Returns the results scrapped with keyword between minimum scrape time and maximum scrape time
def search(keyword, size, minScrapeTime, maxScrapeTime):
    cnx = mysql.connector.connect(user='root', password='Password', port=13306, host='127.0.0.1', database='scraper')
    cursor = cnx.cursor()

    select_image_query = (
        "SELECT * FROM images WHERE keyword = %s AND %s < scrapeTime AND scrapeTime < %s LIMIT %s")

    image_values = (keyword, minScrapeTime, maxScrapeTime, size)
    cursor.execute(select_image_query, image_values)

    results = cursor.fetchall()

    cursor.close()
    cnx.close()

    query_results_DF = pd.DataFrame(results, columns=['image_id', 'imageUrl', 'keyword', 'scrapeTime'])
    print(query_results_DF)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-scrape", "--scrape", help="perform scrape - enter keyword, size and flickr api key", nargs=3)
    parser.add_argument("-search", "--search", help="perform search - enter keyword, size, minScrapeTime and maxScrapeTime", nargs=4)

    args = parser.parse_args()

    if args.scrape:
        scrape(str(args.scrape[0]), int(args.scrape[1]), str(args.scrape[2]))
        sys.exit()
    elif args.search:
        search(str(args.search[0]), int(args.search[1]), args.search[2], args.search[3])
        sys.exit()
    print("no command received - exiting")


#Example execution:
#python scraper.py -search dog 5 "2022-12-03 23:00:00" "2022-12-03 23:55:00" API_KEY

### DOCKER:
# docker run -p 13306:3306 --name mysql-test -e MYSQL_ROOT_PASSWORD=Password -d mysql:5.7
# docker exec -it mysql-test bash
# mysql -u root -pPassword -h 127.0.0.1
# CREATE DATABASE scraper;
# USE scraper;

# ## mysql> CREATE TABLE IF NOT EXISTS images (
#     ->     image_id INT AUTO_INCREMENT PRIMARY KEY,
#     ->     imageUrl VARCHAR(2083) NOT NULL,
#     ->     keyword VARCHAR(255) NOT NULL,
#     ->     scrapeTime DATETIME NOT NULL);
# mysql> describe images;

# CREATE INDEX keyword_idx ON images (keyword);
# SHOW INDEX FROM images;

## SELECT * FROM images;