#!/usr/bin/python3

import datetime
from math import trunc
import sys, optparse
import ArpaeSecrets
from pollconst import *
import csv

#import mysql.connector as MySQLdb
import MySQLdb

# prototipo di estrazione da pds a pde con scrittura in outfile
def getpolldb(pds, pde, outfile):

    pds1 = pds- datetime.timedelta(days=1)
    pde1 = pde- datetime.timedelta(days=1)

#    stazs = readStazs()
#    vars = readVars()

    sql = "select station_id,reftime,\n"
    for i in range(len(vars)):
        sql+=f"max(CASE WHEN variable_id='{vars[i]}' THEN value END) as '{vars[i]}',\n"

    sql = sql.rstrip(sql[-1])
    sql = sql.rstrip(sql[-1])
    sql +="\n"
    
    sql+="from poldat\n"
    sql+=f"WHERE variable_id<'B48100' AND station_id>1 AND reftime BETWEEN '{pds1}' AND '{pde1}'\n"
    sql+="group by station_id,reftime\n"
    sql+="order by station_id,reftime"

    #print(sql)

    cnx = getCnx()
    cursor = cnx.cursor()
    cursor.execute(sql)

    records = cursor.fetchall()
    
    cursor.close()
    cnx.close()
    
    for r in records:
        id=str(r[0])
        dt = str(r[1]+datetime.timedelta(days=1)).replace(' 00:00:00','T00:00:00Z')
        nome = stazs[id]["nome"]
        lat = stazs[id]["archiLat"]
        lon = stazs[id]["archiLon"]
        alt = stazs[id]["alt"]
        

        json = '{"ident": null, "network": "pollini", "version": "0.1", '
        json += f'"date":"{dt}",'

        json += f'"lat":{lat}, "lon":{lon}, "data": [{{"timerange": [0, 0, 86400], "vars": {{'
        
        for j in range(len(vars)):
            if(j>0):
                json += ', '
            if(r[j+2]!= None):
                json += '"' +vars[j] + '":{"v":' + str(trunc(r[j+2])) + '}'  
            else:
                json += '"' +vars[j] + '":{"v":null}'  

        json += '}, "level": [103, 15000, null, null]}, {"vars": {"B01019": {"v": "' + nome + '"}, "B05001": {"v": ' + str(int(lat)/ 100000) + '}, "B06001": {"v":' + str(int(lon)/ 100000) + '}, '
        json += '"B07030": {"v":' + alt + '}, "B01194": {"v": "pollini"}}}]}\n'

        outfile.write(json)
    

def readStazs():
    """Legge il file delle stazioni e ritorna un JSON che ha come chiave il codice stazione"""
    stazs = {}

    with open('confStaz.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            stazId = str(row[0])
            stazs[stazId] = {'nome': row[1], 'archiLat': row[2], 'archiLon': row[3], 'alt':row[4]}
        
    return stazs

def readVars():
    """Legge il file delle variabili e ritorna un array con il codice di ciascuna di esse"""
    vars = []

    with open('confVars.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            vars = row

    return vars


def getCnx():
    """Ritorna una connessione al DB costruita sulle credenziali contenute in ArpaeSecrets nell'oggetto DBpollini"""
    try:
        cnx = MySQLdb.connect(**ArpaeSecrets.DBpollini)
    except MySQLdb.Error as err:
        if err.errno == MySQLdb.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == MySQLdb.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    return cnx


if __name__ == '__main__':
    # command line arguments handling
    clopt = optparse.OptionParser()

    clopt.add_option("-s", "--startdate", metavar="AAAA-MM-GG",
                     help=u"data d'inizio dei dati da estrarre")
    clopt.add_option("-e", "--enddate", metavar="AAAA-MM-GG",
                     help="data di termine dei dati da estrarre")
    clopt.add_option("-o", "--outfile", metavar="STRING",
                     help="nome del file di uscita, default stdout")
    (options, args) = clopt.parse_args()

    if options.enddate is not None:
        de = datetime.datetime.strptime(options.enddate[:10], "%Y-%m-%d")
    else:
        de = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if options.startdate is not None:
        ds = datetime.datetime.strptime(options.startdate[:10], "%Y-%m-%d")
    else:
        ds = de - datetime.timedelta(days=7)
    if options.outfile is not None:
        of = open(options.outfile, mode="w")
    else:
        of = sys.stdout

    getpolldb(ds, de, of)
