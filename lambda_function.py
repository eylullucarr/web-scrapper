from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import psycopg2


def lambda_handler():
    # Google Haberler sayfasını ziyaret etmek için kullanılacak URL
    url = "https://news.google.com/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx1YlY4U0FuUnlHZ0pVVWlnQVAB?hl=tr&gl=TR&ceid=TR%3Atr"

    # Sayfayı çek
    page = requests.get(url).text

    # BeautifulSoup kullanarak sayfayı parse et
    soup = BeautifulSoup(page, "html.parser")

    # Kategorileri çek
    result_category = soup.select("div .BPNpve")
    category = [t.text for t in result_category]

    # Başlıkları çek
    result_tl = soup.select("article.IBr9hb .gPFEn")
    title = [t.text for t in result_tl]

    # Tarih ve zaman bilgilerini çek
    result_dt = soup.select("article.IBr9hb .UOVeFe .hvbAAd")
    timedate = [d["datetime"] for d in result_dt]

    # Kaynak URL'leri çek
    source_urls = []
    base_url = "https://news.google.com/"
    for i in soup.select("article.IBr9hb .WwrzSb"):
        ss = urljoin(base_url, i.get("href"))
        source_urls.append(ss)

    # Verileri toplu bir dizi içinde sakla
    data_length = len(title)
    multiple_array = []
    for index in range(data_length):
        data_entry = {
            "category": category,
            "title": title[index],
            "timedate": timedate[index],
            "source_url": source_urls[index],
            "index": (index + 1),
        }
        multiple_array.append(data_entry)

    # Veritabanı bağlantısı
    conn = psycopg2.connect(
        dbname="news",
        user="postgres",
        password="12345678",
        host="database-1.csajx7yuoxzk.eu-west-1.rds.amazonaws.com",
        port="5432",
    )

    # Bağlantı üzerinden bir imleç oluşturun
    cur = conn.cursor()

    # Veriyi PostgreSQL tablosuna ekleyin
    for data in multiple_array:
        cur.execute(
            "SELECT id FROM news WHERE title = %s OR source_url = %s",
            (data["title"], data["source_url"]),
        )
        existing_id = cur.fetchone()

        if existing_id is None:
            cur.execute(
                "INSERT INTO news (category, title, timedate, source_url, index) VALUES (%s, %s, %s, %s, %s)",
                (
                    data["category"],
                    data["title"],
                    data["timedate"],
                    data["source_url"],
                    data["index"],
                ),
            )

    # Değişiklikleri onaylayın
    conn.commit()

    # İmleci ve bağlantıyı kapatın
    cur.close()
    conn.close()

    return {
        "statusCode": 200,
        "body": "Data inserted into the database.",
    }


lambda_handler()
