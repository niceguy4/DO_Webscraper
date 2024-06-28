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



def createDatabase(dbName: str):
    # Connect/create database
    conn = sqlite3.connect(dbName)
    print(f"Connected to database {dbName}")

    # Create a cursor object
    cursor = conn.cursor()

    # Create a table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS web (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL UNIQUE,
        scraped INTEGER NOT NULL
    )
    ''')
    print("Table 'web' created")

    # Commit and connection
    conn.commit()
    conn.close()
    print(f"Database {dbName} closed")

# Add 1st URL to DB
def startURL(startingURL: str, dbName: str):
    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO web (url, scraped) VALUES (?, ?)""", (startingURL, 0))
    conn.commit()
    conn.close()

def webScrape(url: str) -> list:
    scrapeLoop = True
    failCount = 0
    while scrapeLoop:
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
            scrapeLoop = False
        except Exception as e:
            f = open("error.log", "a")
            f.write(f'\ndatetime.now(): {datetime.now()}\n')
            f.write(f'Error URL: {url}\n')
            f.write(str(e))
            f.close()
            driver.quit()

            failCount = failCount + 1
            if failCount == 2:
                scrapeLoop = False
            print(f"Driver (get) error: {str(e)}")
            time.sleep(5)
    return(url_list)


# Grab URLs that have not been scrapped
def grabTargetURLs(dbName: str) -> list:
    removeTuple = []
    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT url, scraped FROM web WHERE scraped = ?", (0,))
        rows = cursor.fetchall()
        for row in rows:
            removeTuple.append(row[0])
    except:
        conn.close()
        print("Error on grabTargetURLs function")
    conn.close()
    return (removeTuple)

# Randomly select URL from target list
def grabrandomURL(pre_url_list: list) -> str:
    int_rand = random.randrange(0, len(pre_url_list))
    url_final = pre_url_list[int_rand]
    return(url_final)

# Load all URLs from DB
def loadDBList(dbName: str) -> list:
    removeTuple = []
    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    cursor.execute("SELECT url, scraped FROM web")
    rows = cursor.fetchall()
    for row in rows:
        removeTuple.append(row[0])
    conn.close()

    print(f'DB URL Count: {str(len(removeTuple))}')
    return(removeTuple)

# Compare non-duplicate list to DB list
def compareList(url_list: list, current_db_list: list) -> list:
    tempList = []
    for url_item in url_list:
        if url_item not in current_db_list:
            tempList.append([url_item, 0])

    return(tempList)

# Parse scraped URLs to domain level
def parseDomains(vettedList: list) -> list:
    parsedDomains = []
    print('Parsing Domains...')
    for url_item in vettedList:
        parsed_uri = urlparse(url_item[0])
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        parsedDomains.append(domain)
    
    return (parsedDomains)

# Count parsed URL domains and add domains & count to list
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

# Update or add new domain counts in DB
def updateDomainCount(DomainCountList: list, dbName: str):
    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    for item in DomainCountList:
        cursor.execute("""INSERT INTO domains (SLDomain, DCount) VALUES (?, ?)
                       ON CONFLICT (SLDomain) DO UPDATE SET DCount = DCount + ?""", (item[0], item[1], item[1]))
        conn.commit()
    conn.close()

# Load domains and counts to list
def loadDomains(dbName: str) -> list:
    removeTuple = []
    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    cursor.execute("SELECT SLDomain, DCount FROM domains")
    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        removeTuple.append([row[0], row[1]])

    print(f'DB Domain Count: {str(len(removeTuple))}')
    return(removeTuple)

# Write URLs to DB
# If URL is > maxDomainHit, save as 1
# If URL is < maxDomainHit, save as 0
def updateListDB(vettedList, dbName, url, domainsList, maxDomainHit):
    finalList = []
    foundDomain = False
    for fitem in vettedList:
        foundDomain = False
        parsed_uri = urlparse(fitem[0])
        fitemdomain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        for ditem in domainsList:
            if fitemdomain == ditem[0] and ditem[1] > maxDomainHit:
                finalList.append([fitem[0], 1])
                foundDomain = True
                break
            elif fitemdomain == ditem[0] and ditem[1] < maxDomainHit:
                finalList.append([fitem[0], 0])
                foundDomain = True
                break
            else:
                foundDomain = False
        if foundDomain == False:
            finalList.append([fitem[0], 0])

    conn = sqlite3.connect(dbName)
    cursor = conn.cursor()
    for item in finalList:
        cursor.execute("""INSERT OR IGNORE INTO web (url, scraped) VALUES (?, ?)""", (item[0], item[1]))
        conn.commit()
    try:
        cursor.execute("""UPDATE web SET scraped = ? WHERE url = ?""", (1, url))
        conn.commit()
    except:
        print('DB Die!')
    conn.close()

if __name__ == "__main__":
    dbName = 'webscraper.db'
    minWait = 1000 # 1000 = 1s
    maxWait = 2000 # 1000 = 1s
    maxDomainHit = 100
    py_argu = sys.argv
   
    # run .py with 1 argument to generate DB
    # run .py with 2 argument to generate DB & add starting URL
    if len(py_argu) > 1:
        if len(py_argu) == 2:
            createDatabase(dbName)
        elif len(py_argu) == 3:
            startingURL = str(py_argu[2])
            createDatabase(dbName)
            startURL(startingURL, dbName)
 
    while True:
        pre_url_list = grabTargetURLs(dbName)
        if len(pre_url_list) == 0:
            print('Scrapping complete or error.')
            quit()
        url = grabrandomURL(pre_url_list)
        
        print('Target URL: ' + str(url))

        scrapeList = webScrape(url)
        
        # gc improved from 4 seconds to .5 seconds for loadDBList function
        gc.collect()

        current_db_list = loadDBList(dbName)
        vettedList = compareList(scrapeList, current_db_list)

        # Parse URLs for Domains.
        # Add up total Domains and track
        # Update DB for Domains and Counts
        # Reload Domain & Count List
        parseDomainsList = parseDomains(vettedList)
        DomainCountList = countOccurrences(parseDomainsList)
        updateDomainCount(DomainCountList, dbName)
        domainsList = loadDomains(dbName)

        # Use Domain Count Lists to write New URLS to DB
        # So that Domains are not hit more then max hits
        updateListDB(vettedList, dbName, url, domainsList, maxDomainHit)

        randSleep = float((random.randrange(minWait, maxWait)/1000))        
        print(f'Sleeping for {str(randSleep)}\n')
        gc.collect()        
        time.sleep(randSleep)

