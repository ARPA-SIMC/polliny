#!/usr/bin/python3

import datetime
from math import trunc
import sys
import optparse
import json
import csv
import ArpaeSecrets
import anag
try:
    import MySQLdb
except:
    import mysql.connector as MySQLdb


def getpolldb(pds, pde, outfile):
    """Estrae tutti i dati osservati per il periodo richiesto e li scrive
in formato json nel file file object outfile"""

    pds1 = pds - datetime.timedelta(days=1)
    pde1 = pde - datetime.timedelta(days=1)

    sql = "select station_id,reftime,\n"
    for i in range(len(anag.vars)):
        sql+=f"max(CASE WHEN variable_id='{anag.vars[i]}' THEN value END) as '{anag.vars[i]}',\n"

    sql = sql.rstrip(sql[-1])
    sql = sql.rstrip(sql[-1])
    sql +="\n"
    
    sql+="from poldat\n"
    sql+=f"WHERE variable_id<'B48100' AND station_id>1 AND reftime BETWEEN '{pds1}' AND '{pde1}'\n"
    sql+="group by station_id,reftime\n"
    sql+="order by station_id,reftime"

    cnx = getCnx()
    cursor = cnx.cursor()
    cursor.execute(sql)

    records = cursor.fetchall()
    
    cursor.close()
    cnx.close()
    
    for r in records:
        id=str(r[0])
        dt = str(r[1]+datetime.timedelta(days=1)).replace(' 00:00:00','T00:00:00Z')
        nome = anag.statid_to_key(id, "nome")
        lat = anag.statid_to_key(id, "intlat")
        lon = anag.statid_to_key(id, "intlon")
        alt = anag.statid_to_key(id, "alt")
        

        json = '{"ident": null, "network": "pollini", "version": "0.1", '
        json += f'"date":"{dt}",'

        json += f'"lat":{lat}, "lon":{lon}, "data": [{{"timerange": [0, 0, 86400], "vars": {{'
        
        for j in range(len(anag.vars)):
            if(j>0):
                json += ', '
            if(r[j+2]!= None):
                json += '"' +anag.vars[j] + '":{"v":' + str(trunc(r[j+2])) + '}'  
            else:
                json += '"' +anag.vars[j] + '":{"v":null}'  

        json += '}, "level": [103, 15000, null, null]}, {"vars": {"B01019": {"v": "' + nome + '"}, "B05001": {"v": ' + str(int(lat)/ 100000) + '}, "B06001": {"v":' + str(int(lon)/ 100000) + '}, '
        json += '"B07030": {"v":' + alt + '}, "B01194": {"v": "pollini"}}}]}\n'

        outfile.write(json)


def putpolldb(filename):
    """Carica sul database tutti tutti i dati previsti contenuti nel file
filename"""
    print(f"sto importando {filename}")
    cnx = getCnx()
    cursor = cnx.cursor()

    f = open(filename,"r")
    line = f.readline()
    while line:
        
        x=json.loads(line)
        idStaz = anag.coord_to_key(str(x["lon"]), str(x["lat"]), 'idstaz')

        dt = x["date"].replace("T"," ").replace("Z","")

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
    # command line arguments handling
    clopt = optparse.OptionParser(usage="%prog [OPTIONS] <operazione> [file1 [file2...]]", description="""Questo programma estrae
dal database dei pollini tutte le osservazioni per un determinato
periodo (operazione get) oppure carica sul database i file di
previsione forniti (operazione put); i dati in ingresso/uscita
sono sempre in formato dballe json.""")

    clopt.add_option("-s", "--startdate", metavar="AAAA-MM-GG",
                     help=u"data d'inizio dei dati da estrarre")
    clopt.add_option("-e", "--enddate", metavar="AAAA-MM-GG",
                     help="data di termine dei dati da estrarre")
    clopt.add_option("-o", "--outfile", metavar="STRING",
                     help="nome del file di uscita, default stdout")
    (options, args) = clopt.parse_args()


    if len(args) == 0:
        clopt.print_help()
        print("Errore, e' necessario specificare l'operazione")
        sys.exit(1)
    
    oper = args[0]
    if oper == "get":
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

    elif oper == "put":
        for infile in args[1:]:
            putpolldb(infile)
    else:
        clopt.print_help()
        print("Errore, l'operazione deve essere 'get' o 'put'")
        sys.exit(1)
