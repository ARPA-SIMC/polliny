#!/usr/bin/python3

import ArpaeSecrets
from math import trunc
import sys
import json
import csv
from textwrap import indent
try:
    import MySQLdb
except:
    import mysql.connector as MySQLdb


def readStazsArkimet():
    """Legge il file delle stazioni e ritorna un JSON che ha come chiave il codice stazione"""
    stazs = {}

    with open('confStaz.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            stazId = str(row[2])+"_"+str(row[3])
            stazs[stazId] = {'id':row[0],'nome': row[1], 'archiLat': row[2], 'archiLon': row[3], 'alt':row[4]}
        
    return stazs

def setpolldb(filename):
    stazs = readStazsArkimet()
    print(f"sto importando {filename}")
    cnx = getCnx()
    cursor = cnx.cursor()

    f = open(filename,"r")
    line = f.readline()
    while line:
        
        x=json.loads(line)
        k = str(x["lat"]) + "_" + str(x["lon"])
        idStaz = stazs[k]['id']

        dt = x["date"].replace("T"," ").replace("Z","")
        
        #print(idStaz+" "+dt)

        sql = "INSERT INTO `polprev` (`station_id`,`variable_id`,`reftime`,`value`)\n"
        sql += "VALUES\n"

        hasdata = False
        for d in x["data"]:
            if "timerange" in d:
                for v in d['vars']:
                    sql+="(" + idStaz + ",'" + v + "','" + dt + "'," + str(d['vars'][v]['v']) + "),\n"
                    hasdata = True
        
        sql = sql[:-2]
        sql += "\nON DUPLICATE KEY\n"
        sql += "UPDATE modified=IF(isnull(modified),IF(value<>values(value),1,null),IF(value<>values(value),modified+1,modified)),value=values(value)\n"
        
        if hasdata:
            print(sql)
            cursor.execute(sql)
        line = f.readline()
    cnx.commit()

    cursor.close()
    cnx.close()
    return


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

    if (len(sys.argv) != 2):
        print("Indicare il nome del file da caricare in mySQL")



    setpolldb(sys.argv[1])
