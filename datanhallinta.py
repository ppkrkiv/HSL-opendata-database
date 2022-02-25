#!/usr/bin/python
import csv
from os import name
import sqlite3
import requests
import json
from contextlib import closing
import time

def main():
    luoKanta()
    #haeDataa()
    lueAsemaCsv()
    lueMatkaCsv()
    luomyHkiTaulu()      #<--- luo taulu kantaan
    taytamyHkiTaulu()    #<--- täytä taulu tapahtumilla
    createIndex()        #<--- indeksoi matka-taulu 
    #checkData()         #<--- tällä voit varmistaa, että data on oikeasti taulussa
    #dropTable()         #<--- poista jos jotain meni pieleen
    #poistaNahtavyydet() #<--- turhia varten

#
#luo kannan pyörasemille
#sama rakenne kuin csv-tiedostossa
#HUOM!: operaattorin ja kapasiteetin nimet ovat muuttuneet!
#
def luoKanta():
    conn = sqlite3.connect('tietokanta.db')

    conn.execute('''CREATE TABLE ASEMA
            (
            ID            INT     PRIMARY KEY     NOT NULL,
            NIMI          CHAR(20)                NOT NULL,
            OSOITE        CHAR(50)                        ,
            KAUPUNKI      CHAR(15)                        ,
            OPERAATTORI   CHAR(15)                        ,
            KAPASITEETTI  INT                             ,
            X             INT                             ,
            Y             INT                             );''')

    conn.execute('''CREATE TABLE MATKA
            (
            DEPARTURE_TIME          DATETIME              ,
            RETURN_TIME             DATETIME              ,
            DEPARTURE_STATION_ID    INT                   ,
            DEPARTURE_STATION_NAME  CHAR(50)              ,
            RETURN_STATION_ID       INT                   ,
            RETURN_STATION_NAME     CHAR(50)              ,
            DISTANCE_METERS         INT                   ,
            DURATION_SEC            INT                   );''')

    print("Tietokanta luotu")
    conn.close()

#testausta varten
def haeDataa():
    conn = sqlite3.connect('tietokanta.db')
    cursor = conn.execute("SELECT * FROM ASEMA")
    for row in cursor:
        print(row)
    conn.close()

#poistaa taulun
def dropTable():
    conn = sqlite3.connect('tietokanta.db')
    cursor = conn.execute("DROP TABLE NAHTAVYYS")
    cursor.close()
    conn.close()
    print("taulu poistettu kannasta")

#csv:n lukua varten
def lueAsemaCsv():
    columns = ['FID', 'ID', 'Nimi', 'Namn', 'Name', 'Osoite', 'Adress', 'Kaupunki',
               'Stad', 'Operaattor', 'Kapasiteet', 'x', 'y']

    try:
        with open('pyora-asemat.csv', 'r', newline='', encoding='utf-8') as fin:
            dr = csv.DictReader(fin, fieldnames=columns, delimiter=',')
            next(dr)
            asemat = [(i['ID'], i['Nimi'], i['Osoite'], i['Kaupunki'], i['Operaattor'], i['Kapasiteet'], i['x'], i['y']) for i in dr]
            
 
        conn = sqlite3.connect('tietokanta.db')
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO ASEMA \
            (ID, NIMI, OSOITE, KAUPUNKI, OPERAATTORI, KAPASITEETTI, X, Y) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", asemat)

        conn.commit()
        cursor.execute("SELECT * FROM ASEMA")
        result = cursor.fetchall()
        print(result)
        cursor.close()
        conn.close()

    except sqlite3.Error as error:
        print('Error: ', error)
    
    finally:
        print("ASEMA-taulu täytetty")


#saattaa pureskella dataa hetken, kun käyttää
#TODO: -datan karsiminen: lisäysehtoja, joilla karsitaan merkityksettömät matkat
#
#PÄIVITETTY: looppi kaikkien datasettien hakemiseen
#
def lueMatkaCsv():
    columns = ['Departure', 'Return', 'Departure station id', 'Departure station name', 'Return station id',
                  'Return station name', 'Covered distance (m)', 'Duration (sec.)']
    years = [2016, 2017, 2018, 2019, 2020, 2021]
    months = [4, 5, 6, 7, 8, 9, 10]

    for year in years:
        for month in months:
            if year <= years[1] and month == months[0]:
                continue
            else:
                try:
                    url = "https://dev.hsl.fi/citybikes/od-trips-"+str(year)+"/"+str(year)+"-"+'{:0>2}'.format(str(month))+".csv"
                    print(url)
                    with closing(requests.get(url, stream=True)) as r:
                        f = (line.decode('utf-8') for line in r.iter_lines())
                        dr = csv.DictReader(f, fieldnames=columns, delimiter=',')
                        next(dr)
                        matkat = [(i['Departure'], i['Return'], i['Departure station id'], i['Departure station name'], i['Return station id'], i['Return station name'], i['Covered distance (m)'], i['Duration (sec.)']) for i in dr]

                    conn = sqlite3.connect('tietokanta.db')
                    cursor = conn.cursor()
                    cursor.executemany("INSERT INTO MATKA \
                        (DEPARTURE_TIME, RETURN_TIME, DEPARTURE_STATION_ID, DEPARTURE_STATION_NAME, RETURN_STATION_ID, RETURN_STATION_NAME, DISTANCE_METERS, DURATION_SEC) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", matkat)

                    conn.commit()
                    cursor.close()
                    conn.close()

                except sqlite3.Error as error:
                    print('Error: ', error)

                finally:
                    print("MATKA-taulu täytetty ajalta " + str(year) +', ' + str(month))


def luomyHkiTaulu():
    conn = sqlite3.connect('tietokanta.db')
    conn.execute('''CREATE TABLE NAHTAVYYS
            (
            ID            INT     PRIMARY KEY     NOT NULL,
            NIMI          TEXT                            ,
            INFO_URL      TEXT                            ,
            KAUPUNGINOSA  TEXT                            ,
            KUVAUS        TEXT                            ,
            OSOITE        CHAR(50)                        ,
            LAT           INT                             ,
            LON           INT                             );''')

    print("NAHTAVYYS -taulu luotu")
    conn.close()

#datan haku myHelsinki API:sta
#url -muuttujan linkillä voi tutkia data rakennetta
#
#datassa on kolme tasoa:
#meta: "olioiden määrä", pelkkä laskurin antama luku (ei tarvetta)
#data: varsinainen data
#tags: lista tageista (joilla voi linkata data-muuttujassa olevaan tagiin?) (ei tarvetta?)
#
#dataan voidaan viitata seuraavasti:
#muuttuja['osio'][osion indeksi]['osion indeksin sisältö']['osion indeksin sisällön muuttuja']
#data['data'][0]['name']['fi'] <- hakee datan ensimmäisen tapahtuman suomenkielisen nimen

def taytamyHkiTaulu():
    try:
        url = requests.get("http://open-api.myhelsinki.fi/v2/places/?tags_search=Main%20attraction%2CMonument%2CAlvar%20Aalto%2CSculpture%2CConcert")
        text = url.text
        #data dict-muotoon
        data = json.loads(text)
        
        conn = sqlite3.connect('tietokanta.db')
        cursor = conn.cursor()          
        arvot = [(i['id'],
                  i['name']['fi'],
                  i['info_url'],
                  i['location']['address']['neighbourhood'],
                  i['description']['body'],
                  i['location']['address']['street_address'],
                  i['location']['lat'],
                  i['location']['lon']) for i in data['data']]
                  
        cursor.executemany("INSERT INTO NAHTAVYYS \
                            (ID, NIMI, INFO_URL, KAUPUNGINOSA, KUVAUS, OSOITE, LAT, LON) \
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?);", arvot)
        conn.commit()
        cursor.close
        conn.close()

    except sqlite3.Error as error:
        print('Error: ', error)

    finally:
        print("NATHAVYYS -taulu täyetty")  
()


#voi käyttää kaikkiin muihinkin tauluihin
#lisätty ajastin, jolla voi tarkastella funktion viemää aikaa
def checkData():
    start_time = time.time()
    conn = sqlite3.connect('tietokanta.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM NAHTAVYYS;")
    #cursor.execute('SELECT strftime("%Y-%m", DEPARTURE_TIME) as Date FROM MATKA;')
    #cursor.execute('SELECT COUNT(*) FROM MATKA \
    #                WHERE DEPARTURE_TIME LIKE "2021-04-02%";')
    #cursor.execute('SELECT SUM(DISTANCE_METERS) FROM MATKA \
    #                WHERE date(RETURN_TIME) = "2019-07-25";')

    result = cursor.fetchall()
    print("Kyselyn tulos: " + str(result))
    cursor.close()
    conn.close()
    #print("Suoritusaika: %s sekuntia" % (time.time() - start_time))

#datn indeksointi
def createIndex():
    conn = sqlite3.connect('tietokanta.db')
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX idx_ID \
                    ON MATKA (DEPARTURE_STATION_ID, RETURN_STATION_ID);')
    cursor.execute('CREATE INDEX idx_TIME \
                    ON MATKA (DEPARTURE_TIME, RETURN_TIME);')
    cursor.execute('CREATE INDEX idx_DURATION \
                    ON MATKA (DISTANCE_METERS);')
    cursor.execute('CREATE INDEX idx_DISTANCE \
                    ON MATKA (DURATION_SEC);')
    conn.commit()
    cursor.close()
    conn.close()
    print("MATKA-taulu indeksoitu")


#poistaa nähtävyydet, joita ei haluta
def poistaNahtavyydet():
    try:
        conn = sqlite3.connect('tietokanta.db')
        cursor = conn.cursor()
        lause = "DELETE FROM NAHTAVYYS \
                    WHERE ID IN (634,567,331,566,570,3294,1609,147,409,568,2143,564,126,3044,571,197,3002,2577,536,192,565,130,572,573,3337,569,286,1971,2341);"
        cursor.execute(lause)
        conn.commit()

    except sqlite3.Error as error:
        print('Error: ', error)

    finally:
        cursor.execute("SELECT ID FROM NAHTAVYYS \
                        ORDER BY ID ASC;")
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        print(result)


main()