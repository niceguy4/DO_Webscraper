import sqlite3

db_name = "webscraper.db"
item1 = "https://www.naturalreaders.com/"
item2 = 0
conn = sqlite3.connect(db_name)
cursor = conn.cursor()
cursor.execute("""INSERT OR IGNORE INTO web (url, scraped) VALUES (?, ?)""", (item1, item2))
conn.commit()
conn.close()
