import sqlite3
import random
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sys
from urllib.parse import urlparse
from datetime import datetime
#from time import perf_counter
import gc



def create_database(db_name: str):
    # Connect to the database (it will be created if it doesn't exist)
    conn = sqlite3.connect(db_name)
    print(f"Connected to database {db_name}")

    # Create a cursor object
    cursor = conn.cursor()

    # Create a table named 'users's
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS web (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL UNIQUE,
        scraped INTEGER NOT NULL
    )
    ''')
    print("Table 'web' created")

    # Commit the changes and close the connection.
    conn.commit()
    conn.close()
    print(f"Database {db_name} closed")

# Add 1st URL to DB
def add_starting_url(starting_url: str, db_name: str):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO web (url, scraped) VALUES (?, ?)""", (starting_url, 0))
    conn.commit()
    conn.close()

def web_scraper(url: str) -> list:
    url_bool = True
    count_issue = 0
    while url_bool:
        try:
            url_list = []

            userAgent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.114 Safari/537.36'
            options = Options()
            options.add_argument('--headless') 
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument(f"--user-agent={userAgent}")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            options.add_argument('--disable-gpu')

            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            
            # Default set_page_load_timeout 300 seconds. Render timeout change
            driver.set_page_load_timeout(60)
            driver.get(url)
            all_links = driver.find_elements(By.TAG_NAME, 'a')
            
            # Searches for https:// links
            # Does not add duplicate links to list
            print(f'Dateime: {datetime.now()}')
            grab_count = 0 
            for item in all_links:
                try:
                    item_get = (item.get_attribute('href'))
                    if (item_get) is not None:
                        if(item_get.startswith('https://')):
                            grab_count = grab_count + 1
                            if item_get not in url_list:
                                url_list.append(item_get)
                                #print(item.get_attribute('href'))
                except Exception as e:
                    print("except NEXT")
                    print(f'href exception: {e}')
                    break

             # It's a good practice to close the browser when done
            driver.quit()
            print(f'Grab URL Count: {str(grab_count)}')
            print(f'Grab Non-duplicate URL count: {str(len(url_list))}')
            url_bool = False
        except Exception as e:
            f = open("webscrape_error.log", "a")
            f.write(f'\ndatetime.now(): {datetime.now()}\n')
            f.write(f'Error URL: {url}\n')
            f.write(str(e))
            f.close()
            driver.quit()

            count_issue = count_issue + 1
            if count_issue == 2:
                url_bool = False
            print(f"Driver (get) error: {str(e)}")
            time.sleep(5)
    return(url_list)


# Grab only URLs that have not yet been scrapped
def target_url_from_db(db_name: str) -> list:
    db_list_removelist_removetuple = []
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT url, scraped FROM web WHERE scraped = ?", (0,))
        rows = cursor.fetchall()
        for row in rows:
            db_list_removelist_removetuple.append(row[0])
    except:
        conn.close()
        print("Error on target_url_from_db function")
    conn.close()
    return (db_list_removelist_removetuple)

# Randomly select URL from target list
def random_select_list(pre_url_list: list) -> str:
    int_rand = random.randrange(0, len(pre_url_list))
    url_final = pre_url_list[int_rand]
    return(url_final)

# Load URLs from DB to compare to web scrape list
def load_db(db_name: str) -> list:
    db_list_removelist_removetuple = []
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT url, scraped FROM web")
    rows = cursor.fetchall()
    for row in rows:
        db_list_removelist_removetuple.append(row[0])
    conn.close()

    print(f'DB URL Count: {str(len(db_list_removelist_removetuple))}')
    return(db_list_removelist_removetuple)

# Compare non-duplicate list to DB list
def compare_db(url_list: list, current_db_list: list) -> list:
    temp_list = []
    for url_item in url_list:
        if url_item not in current_db_list:
            temp_list.append([url_item, 0])

    return(temp_list)

# Parse Scraped URLs to Domain level
def parseDomains(final_list: list) -> list:
    parsedDomains = []
    print('Parsing Domains...')
    for url_item in final_list:
        parsed_uri = urlparse(url_item[0])
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        parsedDomains.append(domain)
    
    return (parsedDomains)

# Count Parsed URL Domains. Add Domains & Count to list
def countOccurrences(parseDomainsList: list) -> list:
    nonDuplicate = []
    domainListCount = []
    for item in parseDomainsList:
        if item not in nonDuplicate:
            nonDuplicate.append(item)
    
    for url_item in nonDuplicate:
            count = 0
            for ele in parseDomainsList:
                if (ele == url_item):
                    count = count + 1
            domainListCount.append([url_item,count])
            print(f'URL: {url_item} Count: {count}')
    return (domainListCount)

# Update Domain Counts in DB
def updateDomainCount(DomainCountList: list, db_name: str):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    for item in DomainCountList:
        cursor.execute("""INSERT INTO domains (SLDomain, DCount) VALUES (?, ?)
                       ON CONFLICT (SLDomain) DO UPDATE SET DCount = DCount + ?""", (item[0], item[1], item[1]))
        conn.commit()
    conn.close()

# Load Domains and Counts to list
def loadDomains(db_name: str) -> list:
    db_list_removelist_removetuple = []
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT SLDomain, DCount FROM domains")
    rows = cursor.fetchall()
    conn.close()
    for row in rows:
        db_list_removelist_removetuple.append([row[0], row[1]])


    print(f'DB Domain Count: {str(len(db_list_removelist_removetuple))}')
    return(db_list_removelist_removetuple)

# Write URLs to DB and update
def write_completed_db(final_list, db_name, url, loadDomainsList, maxDomainHit):
    write_finallist = []
    in_domain_list = False
    for fitem in final_list:
        in_domain_list = False
        parsed_uri = urlparse(fitem[0])
        fitemdomain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        for ditem in loadDomainsList:
            if fitemdomain == ditem[0] and ditem[1] > maxDomainHit:
                write_finallist.append([fitem[0], 1])
                in_domain_list = True
                break
            elif fitemdomain == ditem[0] and ditem[1] < maxDomainHit:
                write_finallist.append([fitem[0], 0])
                in_domain_list = True
                break
            else:
                in_domain_list = False
        if in_domain_list == False:
            write_finallist.append([fitem[0], 0])

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    for item in write_finallist:
        cursor.execute("""INSERT OR IGNORE INTO web (url, scraped) VALUES (?, ?)""", (item[0], item[1]))
        conn.commit()
    try:
        cursor.execute("""UPDATE web SET scraped = ? WHERE url = ?""", (1, url))
        conn.commit()
    except:
        print('DB Die!')
    conn.close()

if __name__ == "__main__":
    db_name = 'webscraper.db'
    int_low_wait_range = 1000 # 1000 = 1s
    int_high_wait_range = 2000 # 1000 = 1s
    maxDomainHit = 100
    py_argu = sys.argv
   
    # run .py with 1 argument to generate DB
    # run .py with 2 argument to generate DB & add starting URL
    if len(py_argu) > 1:
        if len(py_argu) == 2:
            create_database(db_name)
        elif len(py_argu) == 3:
            starting_url = str(py_argu[2])
            create_database(db_name)
            add_starting_url(starting_url, db_name)
 
    while True:
        pre_url_list = target_url_from_db(db_name)
        if len(pre_url_list) == 0:
            print('Scrapping complete or error.')
            quit()
        url = random_select_list(pre_url_list)
        
        print('Target URL: ' + str(url))

        web_url_list = web_scraper(url)
        
        # gc improved from 4 seconds to .5 seconds for load_db function
        gc.collect()

        current_db_list = load_db(db_name)
        final_list = compare_db(web_url_list, current_db_list)

        # Parse URLs for Domains.
        # Add up total Domains and track
        # Update DB for Domains and Counts
        # Reload Domain & Count List
        parseDomainsList = parseDomains(final_list)
        DomainCountList = countOccurrences(parseDomainsList)
        updateDomainCount(DomainCountList, db_name)
        loadDomainsList = loadDomains(db_name)

        # Use Domain Count Lists to write New URLS to DB
        # So that Domains are not hit more then max hits
        write_completed_db(final_list, db_name, url, loadDomainsList, maxDomainHit)

        float_rand_sleep_delay = float((random.randrange(int_low_wait_range, int_high_wait_range)/1000))        
        print(f'Sleeping for {str(float_rand_sleep_delay)}\n')
        gc.collect()        
        time.sleep(float_rand_sleep_delay)

