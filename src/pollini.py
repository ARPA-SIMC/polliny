#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, sys, shutil, time, optparse, glob
import datetime
import subprocess
import random
import ftplib
import tempfile
import csv,MySQLdb
# sostituire MySQLdb con pymysql

class MeteoFile:
    def __init__(self, name, rename):
        self.name = name
        self.rename = rename

class GetMeteo:
    def __init__(self, prov, ds, de, adir, rdir, wdir):
        self.ds = ds # start date
        self.de = de # end date
        self.adir = adir # archive dir
        self.rdir = rdir # retrieve dir
        self.wdir = wdir # work dir
        try: os.makedirs(rdir)
        except: None
        try: os.makedirs(wdir)
        except: None
        datename = (de+datetime.timedelta(days=1)).strftime("%Y%m%d")
        self.fileset = (MeteoFile(datename+'_PRE.zip', '_meteo2.csv'),)
        self.status0 = {}
        self.status1 = {}
        self.fileset[0].status = {}
        self.prov = prov
        # data d'inizio delle serie storiche
        self.histds = datetime.datetime(1991, 1, 1)
        for p in prov: self.status0[p] = ST_NOOBS
        for p in prov: self.status1[p] = ST_NOOBS
        for p in prov: self.fileset[0].status[p] = ST_NOOBS

    def retrievehistdata(self):
        for p in self.prov:
            if self.status0[p] == ST_NOOBS:
                if self.__db_get__(self.histds, self.ds-datetime.timedelta(days=1), p,
                                     os.path.join(self.wdir,p+'_meteo0.csv')) == 0:
                    self.status0[p] = ST_OKOBS


    def retrievelastdata(self):
        for p in self.prov:
            if self.status1[p] == ST_NOOBS:
                if self.__db_get__(self.ds, self.de, p,
                                     os.path.join(self.wdir,p+'_meteo1.csv')) == 0:
                    self.status1[p] = ST_OKOBS


    def retrievedata(self):
        self.retrievelastdata()
        if any(self.fileset[0].status[p] < ST_OKOBS 
               for p in self.fileset[0].status.iterkeys()):
            self.__fs_get__()


    def __db_get__(self, ds, de, p, filename):
        try:
            file = open(filename, 'wb')
            csvwf = csv.writer(file)
            db = MySQLdb.connect(host=secrets.pragahost, user=secrets.pragauser, \
                                     passwd=secrets.pragapasswd, db=secrets.pragadb)

            curs = db.cursor()
            table='q'+prov_to_gias[p]+'g'
            csvwf.writerow(mysqlGetColList(curs, table))
#            curs.execute('SELECT * from '+table+ \
#                             ' where tempo >= %s and tempo <= %s order by tempo', (ds,de))
            curs.execute('SELECT TEMPO, ET_H_SMR, VENTO_SMR, VD_SMR, VMAX_SMR, TEMP_MIN_SMR, TEMP_MEDIA_SMR, TEMP_MAX_SMR, UMIN_SMR, UMAX_SMR, U_R_SMR, PREC_SMR, RAD_SMR from '+table+ \
                             ' where tempo >= %s and tempo <= %s order by tempo', (ds,de))
            while True:
                line = curs.fetchone()
                if line is None: break
#                csvwf.writerow((line[0].strftime('%d/%m/%Y'),)+line[1:])
                csvwf.writerow((line[0].strftime('%Y-%m-%d'),)+line[1:])
            err = 0
            file.close()
        except MySQLdb.Error:
            raise
            err = 1
            file.close()
            try: os.unlink(filename)
            except: pass
        try:
            curs.close()
            db.close()
        except MySQLdb.Error:
            pass
        return err


    def __ftp_get__(self):
        todo = False
        for file in self.fileset:
            if any(file.status[p] < ST_OKOBS for p in file.status.iterkeys()): todo = True
        if not todo: return

        savedir = os.getcwd()
        os.chdir(self.rdir)
        try:
            ftp = ftplib.FTP(secrets.iftphost)
            try:
                ftp.login(secrets.iftpuser,secrets.iftppasswd)
                ftp.cwd('ModelloPollini')
                for file in self.fileset:
                    if any(file.status[p] < ST_OKOBS for p in file.status.iterkeys()):
                        try:
                            tmpzipfile = tempfile.NamedTemporaryFile(mode='w+b',
                                                                     suffix='.zip',
                                                                     dir=os.getcwd())
                            ftp.retrbinary('RETR '+file.name, tmpzipfile.write)
                            tmpzipfile.flush() # flush file before unzipping
                            for f in glob.glob('????.txt'): os.unlink(f)
# warning, it may not be possible to reopen tmpzipfile here
                            res = subprocess.call(['unzip', '-o', tmpzipfile.name])
                            tmpzipfile.close()
                            for f in glob.glob('????.csv'):
                                p = f.split('.')[0]
                                if p in file.status.iterkeys():
                                    if file.status[p] < ST_OKOBS:
                                        shutil.copy(f, os.path.join(self.wdir,p+file.rename))
                                        os.unlink(f)
                                        file.status[p] = ST_OKOBS
                        except:
                            print "problema durante trasferimento ftp, continuo"
#                        finally:
            except:
                print "problema durante ftp, continuo"
                ftp.quit()
        except:
            print "problema sul server ftp, continuo"
        os.chdir(savedir)


    def __fs_get__(self):
        todo = False
        for file in self.fileset:
            if any(file.status[p] < ST_OKOBS for p in file.status.iterkeys()): todo = True
        if not todo: return

        savedir = os.getcwd()
        os.chdir(self.rdir)
        for file in self.fileset:
            if any(file.status[p] < ST_OKOBS for p in file.status.iterkeys()):
                    try:
                        srcfile = os.path.join(secrets.fspath, file.name)
                        if os.access(srcfile, os.R_OK):
#                        if os.is_file(srcfile):
                            for f in glob.glob('????.csv'): os.unlink(f)
                            res = subprocess.call(['unzip', '-o', srcfile])
                            for f in glob.glob('????.csv'):
                                p = f.split('.')[0]
                                if p in file.status.iterkeys():
                                    if file.status[p] < ST_OKOBS:
                                        shutil.copy(f, os.path.join(self.wdir,p+file.rename))
                                        os.unlink(f)
                                        file.status[p] = ST_OKOBS
                    except:
                        print "problema durante ricerca file, continuo"
        os.chdir(savedir)


    def getstatus(self, p):
        return  min(self.status0.get(p, 0), self.status1.get(p, 0),
                  self.fileset[0].status.get(p, 0))
#        return  min(self.status0.get(p, 0), self.fileset[0].status.get(p, 0),
#                  self.fileset[1].status.get(p, 0))


    def setstatus(self, p, st):
        if p in self.status0: self.status0[p] = st
        if p in self.status1: self.status1[p] = st
        if p in self.fileset[0].status: self.fileset[0].status[p] = st
#        if p in self.fileset[1].status: self.fileset[1].status[p] = st


class GetPollini:
    def __init__(self, prov, fam, ds, de, adir, rdir, wdir, frac=100, fg=False):
        # go ahead because dates are shifted back (agro data model)
        self.ds = ds + datetime.timedelta(days=1) # start date
        self.de = de + datetime.timedelta(days=1) # end date
        self.adir = adir # archive dir
        self.rdir = rdir # retrieve dir
        self.wdir = wdir # work dir
        try: os.makedirs(rdir)
        except: None
        try: os.makedirs(wdir)
        except: None
        self.status0 = {}
        self.status1 = {}
        self.prov = prov
        self.fam = fam
        for p in prov: self.status0[p] = ST_NOOBS
        for p in prov: self.status1[p] = ST_NOOBS
        self.frac = frac # fraction of data required for starting
        self.fg = fg # whether not to download current observations
        self.reportname = 'rapporto.txt'
        # data immediatamente successiva al momento in cui termina l'archivio pollini
        self.noarchds = datetime.datetime(2009,1,1,0,0) + datetime.timedelta(days=1)


    def create_template(self):
        for f in self.fam:
            t = open(os.path.join(self.wdir,'tabella_'+f.upper()+'.csv'), 'w')
            t.write('"Stazione","G1","G2","G3","G4","G5","G6","G7","S","Tendenza"\n')
            for p in self.prov:
                t.write('"%s",NA,NA,NA,NA,NA,NA,NA,NA,NA\n' % (p,))
            t.close()


    def retrievehistdata(self, prov_string, lon_string, lat_string, provfile_string):
        for p in self.prov: # copy files from archive
            if os.path.exists(os.path.join(self.adir, p+'_pollini0.csv')):
                shutil.copy(os.path.join(self.adir, p+'_pollini0.csv'), self.wdir)
#                self.status0[p] = ST_OKOBS

        # change to retrievedir
        savedir = os.getcwd()
        os.chdir(self.rdir)

        self.__v7d_pollini__(prov_string, lon_string, lat_string, provfile_string,
                             self.noarchds,
                             self.ds-datetime.timedelta(days=1))

        for p in self.prov: # append to files from archive
            if os.path.exists(p+'_pollini1.csv') and \
                    os.path.exists(os.path.join(self.wdir, p+'_pollini0.csv')):

                # append skipping first line
                app = open(os.path.join(self.wdir, p+'_pollini0.csv'), 'a')
                inp = open(p+'_pollini1.csv', 'r')
                line = inp.readline()
                for line in inp.readlines():
                    app.write(line)
                app.close()
                self.status0[p] = ST_OKOBS
                # remove after append
                os.unlink(p+'_pollini1.csv')
            elif os.path.exists(p+'_pollini1.csv'):
                shutil.copy(p+'_pollini1.csv', self.wdir)
                os.unlink(p+'_pollini1.csv')

        os.chdir(savedir)


    def retrievedata(self, prov_string, lon_string, lat_string, provfile_string):
        if self.fg:
            for p in self.prov:
                self.status1[p] = ST_OKOBS
        else:
            # change to retrievedir
            savedir = os.getcwd()
            os.chdir(self.rdir)

            self.__v7d_pollini__(prov_string, lon_string, lat_string, provfile_string, 
                                     self.ds, self.de)

            self.__parse_report__()

            os.chdir(savedir)


    def __v7d_pollini__(self, prov_string, lon_string, lat_string, provfile_string,
                            ds, de, namlname='pollini.naml'):
        """Pulisce i file rimasti; crea la namelist namlname, default
        pollini.naml, per il programma v7d_pollini, per fare
        un'estrazione nell'intervallo temporale d1-d2, scrivendo un
        file di report reportname; esegue il programma v7d_pollini."""

        bufrname = 'pollini.bufr'
# Remove old output files and report file
        for file in provfile_string:
            try: os.unlink(file)
            except: pass
        try: os.unlink(self.reportname)
        except: pass
        try: os.unlink(bufrname)
        except: pass
        print("query+naml: ",ds.isoformat(" "),"-",de.isoformat(" "))

        p1 = subprocess.Popen(['arki-query', '--data',
                               'Reftime:>='+ds.isoformat(' ')+',<='+de.isoformat(' ')+';product:VM2:tr=0,p1=0;',
                               secrets.arkiossurl],
                              stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['meteo-vm2-to-bufr'], stdin=p1.stdout,
                              stdout=open(bufrname,'w'))
        p1.stdout.close()
        p2.communicate()
        try:
            l = os.stat(bufrname).st_size
            print("length of bufr file",l)
            if l <= 0: return
        except:
            return

        
# Create the namelist
  # 'B15230', 'B15254',
  # 'B15238',
  # 'B15201', 'B15202', 'B15203',
  # 'B15221', 'B15222',
  # 'B15204', 'B15205', 'B15206',
  # 'B15207', 'B15208', 'B15209',
  # 'B15219', 'B15220',
  # 'B15210', 'B15211', 'B15212', 'B15213',
  # 'B15200', 'B15247',
  # 'B15214', 'B15215', 'B15216',
  # 'B15231', 'B15255',
  # 'B15217', 'B15248',
  # 'B15229', 'B15253',
  # 'B15232', 'B15233', 'B15234',
  # 'B15240',
  # 'B15226', 'B15227', 'B15228',
  # 'B15218', 'B15249',
        open(namlname, "w").write("""
&POLLINI
 variabili =
  'B48031', 'B48055',
  'B48039',
  'B48002', 'B48003', 'B48004',
  'B48022', 'B48023',
  'B48005', 'B48006', 'B48007',
  'B48008', 'B48009', 'B48010',
  'B48020', 'B48021',
  'B48011', 'B48012', 'B48013', 'B48014',
  'B48001', 'B48048',
  'B48015', 'B48016', 'B48017',
  'B48032', 'B48056',
  'B48018', 'B48049',
  'B48030', 'B48054',
  'B48033', 'B48034', 'B48035',
  'B48041',
  'B48027', 'B48028', 'B48029',
  'B48019', 'B48050',
 famiglie =
  'Aceracee', 'Aceracee',
  'Alternaria',
  'Betulacee', 'Betulacee', 'Betulacee',
  'ChenopodiaceeAmarantacee', 'ChenopodiaceeAmarantacee',
  'Composite', 'Composite', 'Composite',
  'Corilacee', 'Corilacee', 'Corilacee',
  'CupressaceeTaxacee', 'CupressaceeTaxacee',
  'Fagacee', 'Fagacee', 'Fagacee', 'Fagacee',
  'Graminacee', 'Graminacee',
  'Oleacee', 'Oleacee', 'Oleacee',
  'Pinacee', 'Pinacee',
  'Plantaginacee', 'Plantaginacee',
  'Platanacee', 'Platanacee',
  'Salicacee', 'Salicacee', 'Salicacee',
  'Stemphylium',
  'Ulmacee', 'Ulmacee', 'Ulmacee',
  'Urticacee', 'Urticacee'

 stazioni = %s
 file_stazioni = %s
 lonlist = %s
 latlist = %s

 data_inizio = '%s'
 data_fine   = '%s'

 file_rapporto = '%s'
/END
""" % (prov_string, provfile_string, lon_string, lat_string, ds.isoformat(' '), de.isoformat(' '), self.reportname))

# Run v7d_pollini
        res = subprocess.call(['/usr/libexec/libsim/v7d_pollini',bufrname])


    def __parse_report__(self):
        """Interpreta il file di report generato da v7d_pollini,
        aggiornando lo stato delle osservazioni di ogni provincia
        in self.status."""
        try:
            fd = open(self.reportname,"r")
        except:
            print "problema con il file",self.reportname
            return
        for line in fd.readlines():
            w = line.split()
            prov = w[0].split('_')[0]
            frac = int(w[1])
            if prov in self.status1:
                if self.status1[prov] < ST_OKOBS:
                    if frac < self.frac:
                        self.status1[prov] = ST_FEWOBS
                    else:
                        self.status1[prov] = ST_OKOBS
                        shutil.copy(w[0], self.wdir)
                        os.unlink(w[0])
        fd.close()


class RunControl:
    def __init__(self, prov, fam, dir, rscript, fg):
        self.prov = prov
        self.fam = fam
        self.dir = dir
        self.rscript = rscript
        self.fg = fg
        self.ds = meteo.de+datetime.timedelta(days=1)
#        for f in glob.glob(os.path.join(dir,'*.POL')): os.unlink(f)

    def run_forecast(self, meteo, pollini):
        """Sottopone al gestore dei processi della macchina i job che eseguono
        le procedure R di Stefano Marchesi per la previsione dei
        pollini sulle provincia per cui i dati sono disponibili, per
        tutte le famiglie
        """
        if self.fg: fg_env = "TRUE"
        else: fg_env = "FALSE"
        for p in self.prov:
            if meteo.getstatus(p) == ST_OKOBS and \
                    pollini.status0.get(p, 0) == ST_OKOBS and \
                    pollini.status1.get(p, 0) == ST_OKOBS: # can start
                meteo.setstatus(p, ST_RUNFC)
                for f in self.fam:
                    endname = "pollini_%s_%s.end" % (f.upper(), p)

                    job = """#!/bin/sh
#SBATCH --output=pollini_%s_%s_%%j.out
#SBATCH --error=pollini_%s_%s_%%j.out
#SBATCH --partition=serial
#SBATCH --ntasks=1

cd %s
POLLINI_STAZ=%s POLLINI_FAM=%s POLLINI_ANNO=%d POLLINI_MESE=%d POLLINI_GIORNO=%d POLLINI_FG=%s POLLINI_INPUTPATH=./ POLLINI_OUTPUTPATH=./ R CMD BATCH --no-save --no-restore %s /dev/stdout
touch %s
""" % (f.upper(), p, f.upper(), p, self.dir, p, f.upper(), self.ds.year, self.ds.month, self.ds.day, fg_env, self.rscript, endname)
                    try: os.unlink(endname)
                    except: pass
                    jobname = "pollini_%s_%s.job" % (f.upper(), p)
                    j = open(jobname, "w")
                    j.write(job)
                    j.close()
                    subprocess.call(["sbatch",jobname])
                print "Partita la previsione per la provincia",p


    def check_forecast(self, meteo, no_transfer=False):
        for p in self.prov:
            if meteo.getstatus(p) == ST_RUNFC:
                for f in self.fam:
                    if not os.path.exists("pollini_%s_%s.end" % (f.upper(), p)):
                        break
                else: # all endfiles present
                    print "Terminata la previsione per la provincia",p
                    meteo.setstatus(p, ST_ENDFC)

            if meteo.getstatus(p) == ST_ENDFC:
                provprev = self.__tab_conv__("tabella_%s_%s.POL", p)
                print "Creata la previsione BUFR per la provincia",p,"file",provprev
                if no_transfer:
                    meteo.setstatus(p, ST_TRANS)
                else:
                    print "Trasferisco la previsione per la provincia",p
#                    if self.__ftp_put__("tabella_%s_%s.POL", p) == 0:
#                        print "Trasferita la previsione per la provincia",p
#                        meteo.setstatus(p, ST_TRANS)
                    if os.path.exists(provprev):
                        if self.__ftp_put_prov__(provprev) == 0:
                            print "Trasferita la previsione per la provincia",p
                            meteo.setstatus(p, ST_TRANS)

        try:
            return all(meteo.getstatus(p) >= ST_TRANS for p in self.prov)
        except KeyError:
            return False


    def __ftp_put__(self, filetmpl, p):
        errstatus = 0
        try:
            ftp = ftplib.FTP('secrets.oftphost)
            ftp.login(secrets.oftpuser,secrets.oftppasswd)
        except:
            print "Error in ftp connection to "+secrets.ftphost
            return 1

        for f in self.fam:
            name = filetmpl % (f.upper(), p)
            if os.path.exists(name):
                print "Tento di trasferire",name
                try:
                    ftp.storbinary('STOR '+name, open(name, "rb"))
                except:
                    print "Error in transferring ",name
                    errstatus = 1
        try:
            ftp.quit()
        except:
            print "Error in ftp closing"
            errstatus = 1
        return errstatus


    def __ftp_put_prov__(self, filename):
        errstatus = 0
        try:
            ftp = ftplib.FTP('secrets.oftphost)
            ftp.login(secrets.oftpuser,secrets.oftppasswd)
        except:
            print "Error in ftp connection to "+secrets.ftphost
            return 1

        if os.path.exists(filename):
            print "Tento di trasferire",filename
            try:
                ftp.storbinary('STOR '+filename, open(filename, "rb"))
            except:
                print "Error in transferring ",filename
                errstatus = 1
        try:
            ftp.quit()
        except:
            print "Error in ftp closing"
            errstatus = 1
        return errstatus


    def __tab_conv__(self, filetmpl, p):
        outfile = "pollini_prev_%s_%s.bufr" % (p,self.ds.strftime("%Y%m%d%H%M"))
        ana = "%f,%f,pollini" % (float(prov_to_lon[p])/1.E5,float(prov_to_lat[p])/1.E5)
#        ofd = open("pollini_prev_%s.bufr" % (p,),"w")
        dbcsv = []

        for f in self.fam:
            name = filetmpl % (f.upper(), p)
            if os.path.exists(name):
# 2018-11-01 00:00:00,4189,B48019,259200,1,,,000000000
# 12.052,44.027,synop,2013-03-15 06:00:00,103,2000,,,254,0,0,B12101,275.8
# if self.fg: add "bad quality" flag
                for row in csv.reader(open(name), delimiter=","):
                    dbcsv.append("%s,%s,103,15000,,,0,%s,86400,%s,%s\n" %
                                 (ana,row[0],row[3],row[2],row[4]))
        p1 = subprocess.Popen(["dbamsg","convert","-t","csv","-d","bufr"],
                              stdin=subprocess.PIPE,
                              stdout=open(outfile,'w'))

        p1.communicate(''.join(dbcsv))
        return outfile
    

def mysqlGetColList(curs, table):
    collist = ['TEMPO', 'ET_H_SMR', 'VENTO_SMR', 'VD_SMR', 'VMAX_SMR', 'TEMP_MIN_SMR', 'TEMP_MEDIA_SMR', 'TEMP_MAX_SMR', 'UMIN_SMR', 'UMAX_SMR', 'U_R_SMR', 'PREC_SMR', 'RAD_SMR']
    # collist = []
    # curs.execute('DESCRIBE '+table)
    # while True:
    #     line = curs.fetchone()
    #     if line is None: break
    #     collist.append(line[0].upper())
    return collist


# ====================

# codici di stato
ST_NOOBS = 0  # 0 no observations available
ST_FEWOBS = 1 # 1 some observations available but less than required (minobs%)
ST_OKOBS = 2  # 2 enough observations available (unused)
ST_RUNFC = 3  # 10 enough observations available, forecast running
ST_ENDFC = 4  # 20 forecast terminated successfully
ST_TRANS = 5  # 30 forecast successfully transferred to DB

# command line options handling
clopt = optparse.OptionParser()

clopt.add_option("-a", "--archivedir", metavar="DIR", 
                 default=os.path.join(os.environ['HOME'],'archive'),
                 help="sottodirectory che contiene i file con i dati storici archiviati, default %default")
clopt.add_option("-b", "--basedir", metavar="DIR",  default=os.environ['HOME'],
                 help="directory principale che contiene tutte le sottodirectory di lavoro indicate di seguito, default %default")
clopt.add_option("-r", "--retrievedir", metavar="DIR", default='retrieve',
                 help="sottodirectory in cui vengono temporaneamente scaricati i file con i dati degli ultimi giorni, default %default")
clopt.add_option("-w", "--workdir", metavar="DIR", default='work',
                 help="sottodirectory di lavoro, in cui vengono creati i file di ingresso e viene lanciata la procedura di previsione, default %default")
clopt.add_option("", "--rscript", metavar="PATH",
                 default='src/test_operativo_bufr.R',
                 help="nome della script R da richiamare per effettuare la procedura di previsione, default %default")

clopt.add_option("-s", "--startdate", metavar="AAAA-MM-GG",
                 help=u"data d'inizio della osservazioni \"fresche\", default lunedì della settimana precedente")
clopt.add_option("-e", "--enddate", metavar="AAAA-MM-GG",
                 help="data di termine delle osservazioni \"fresche\" e inizio delle previsioni, default domenica della settimana precedente")
clopt.add_option("-d", "--opdate", metavar="AAAA-MM-GG",
                 help="determina le date di inizio e termine come se la procedura fosse eseguita operativamente nel giorno indicato, alternativa a startdate e enddate")
clopt.add_option("", "--ope", action="store_true", default=False,
                 help=u"lavora in modalità operativa, eseguendo e ripetendo automaticamente tutto il ciclo delle operazioni")
clopt.add_option("", "--first-guess", action="store_true", default=False,
                 help=u"esegue le operazioni senza scaricare e utilizzare le osservazioni polliniche dell'ultima settimana, ha senso solo in modalità operativa")
clopt.add_option("-m", "--work-time", metavar="hours", default="72",
                 help=u"tempo massimo in ore durante il quale la previsione resta in attesa dei nuovi dati, ha senso solo in modalità operativa, default %default")
clopt.add_option("-n", "--no-transfer", action="store_true", default=False,
                 help=u"non effettua il trasferimento dei risultati sul database, ha senso solo in modalità operativa")
clopt.add_option("", "--get-meteo", action="store_true", default=False,
                 help=u"prepara i dati meteo in workdir, ha senso solo in modalità non operativa")
clopt.add_option("", "--get-pollini", action="store_true", default=False,
                 help=u"prepara i dati pollini in workdir, ha senso solo in modalità non operativa")
clopt.add_option("", "--hist", action="store_true", default=False,
                 help=u"prepara solo i dati storici di meteo e/o pollini, ha senso solo in modalità non operativa")
#clopt.add_option("", "--create-template", action="store_true", default=False,
#                 help=u"crea i file template per le previsioni di pollini in workdir, ha senso solo in modalità non operativa")
(options, args) = clopt.parse_args()

adir = os.path.abspath(options.archivedir)
rdir = os.path.abspath(os.path.join(options.basedir,options.retrievedir))
wdir = os.path.abspath(os.path.join(options.basedir,options.workdir))
#tt = time.strptime(options.max_time, "%H:%M")
max_time = datetime.datetime.now() + datetime.timedelta(hours=int(options.work_time))


# definizione delle stazioni osservative pollini
# tabella conversione id Oracle/sigla provincia
prov_to_ora = {
    'PC1':'4176', #00369 Piacenza
    'PR2':'4177', #00774 Parma
    'RE1':'4178', #00977 Reggio Emilia
    'MO1':'4179', #01138 Modena
    'MO2':'4180', #01221 Vignola
    'BO1':'4181', #01421 Bologna
    'FE1':'4182', #01574 Ferrara
    'RA2':'4184', #01786 Faenza
    'RA3':'4185', #01983 Ravenna
    'FO1':'4186', #01867 Forlì
    'FO2':'4187', #01989 Cesena
    'FO3':'4188', #02231 Rimini
    'BO3':'4189', #01338 San Giovanni Persiceto
    'BO5':'4190'  #01617 San Pietro Capofiume
    }
prov_to_gias = {
    '4176':'00369', # Piacenza
    '4177':'00774', # Parma
    '4178':'00977', # Reggio Emilia
    '4179':'01138', # Modena
    '4180':'01221', # Vignola
    '4181':'01421', # Bologna
    '4182':'01574', # Ferrara
    '4184':'01786', # Faenza
    '4185':'01983', # Ravenna
    '4186':'01867', # Forlì
    '4187':'01989', # Cesena
    '4188':'02231', # Rimini
    '4189':'01338', # San Giovanni Persiceto
    '4190':'01617'  # San Pietro Capofiume
    }

prov_to_lon = {
    '4176':'970554', # Piacenza
    '4177':'1031667', # Parma
    '4178':'1059917', # Reggio Emilia
    '4179':'1097000', # Modena
    '4180':'1100414', # Vignola
    '4181':'1132820', # Bologna
    '4182':'1161667', # Ferrara
    '4184':'1189148', # Faenza
    '4185':'1220555', # Ravenna
    '4186':'1202757', # Forlì
    '4187':'1223505', # Cesena
    '4188':'1261667', # Rimini
    '4189':'1119489', # San Giovanni Persiceto
    '4190':'1161667'  # San Pietro Capofiume
    }

prov_to_lat = {
    '4176':'4506052', # Piacenza
    '4177':'4478333', # Parma
    '4178':'4465546', # Reggio Emilia
    '4179':'4468389', # Modena
    '4180':'4450405', # Vignola
    '4181':'4450122', # Bologna
    '4182':'4483333', # Ferrara
    '4184':'4428688', # Faenza
    '4185':'4446535', # Ravenna
    '4186':'4422698', # Forlì
    '4187':'4413971', # Cesena
    '4188':'4403333', # Rimini
    '4189':'4463290', # San Giovanni Persiceto
    '4190':'4465000'  # San Pietro Capofiume
    }

# sottoelenco ragionevole operativo ==> cambiare questo
prov_sigla = ('BO1', 'BO3', 'BO5', 'FE1', 'FO1', 'FO2', 'FO3', 'MO1', 'PC1', 'PR2', 'RA2', 'RA3', 'RE1')
#prov_sigla = ('RE1',)
prov = tuple("%s" % prov_to_ora[sigla] for sigla in prov_sigla)
# righe per namelist
prov_string = "%s,"*len(prov) % prov
lon_string = "'%s',"*len(prov) % tuple("%s" % prov_to_lon[cod] for cod in prov)
lat_string = "'%s',"*len(prov) % tuple("%s" % prov_to_lat[cod] for cod in prov)
provfile_string = "'%s_pollini1.csv',"*len(prov) % prov
# definire una classe "anagrafica" per gestire tutto cio`

# famiglie coinvolte (fare il calendario)
fam = ('Aceracee', 'Alternaria', 'Betulacee', 'ChenopodiaceeAmarantacee',
       'Composite', 'Corilacee', 'CupressaceeTaxacee', 'Fagacee', 'Graminacee',
       'Oleacee', 'Pinacee', 'Plantaginacee', 'Platanacee', 'Salicacee',
       'Stemphylium', 'Ulmacee', 'Urticacee')

#fam = ('Alternaria', 'ChenopodiaceeAmarantacee',
#       'Composite', 'Graminacee',
#       'Pinacee',
#       'Stemphylium','Urticacee')

# minima percentuale di osservazioni disponibili per partire con le previsioni
# 4 giorni su 7
minobs = 57

# operational section
# find last sunday 23UTC and the previous one, monday 00UTC
# is not used in order to get 7 days and not 8 (observations
# are exactly at 00UTC)
if options.enddate is not None: #any date
    de = datetime.datetime(*time.strptime(options.enddate, "%Y-%m-%d")[0:3])
elif options.opdate is not None: # pseudo-operational date
    de = datetime.datetime(*time.strptime(options.opdate, "%Y-%m-%d")[0:3])
    de = de.replace(hour=0, minute=0, second=0, microsecond=0)
    de = de - datetime.timedelta(hours=(de.weekday()+1)*24)
else: # operational date
    # find datetime of previous Sunday 00 UTC
    # remember the 1 day shift in the database!
    de = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    # de = de - datetime.timedelta(hours=de.weekday()*24+1)
    de = de - datetime.timedelta(hours=(de.weekday()+1)*24)

if options.startdate is not None and option.opdate is None:
    ds = datetime.datetime(*time.strptime(options.startdate, "%Y-%m-%d")[0:3])
else:
    # go back 6 days, not 7 because retrieve includes extremes
    ds = de - datetime.timedelta(hours=144)

print "Periodo osservazioni:",ds.isoformat(" "),"-",de.isoformat(" ")
#sys.exit(0)
if options.ope or options.get_meteo:
    meteo = GetMeteo(prov, ds, de, adir, rdir, wdir)
if options.ope or options.get_pollini:
    pollini = GetPollini(prov, fam, ds, de, adir, rdir, wdir, minobs, options.first_guess)
if options.ope:
    run = RunControl(prov, fam, wdir, options.rscript, options.first_guess)

os.chdir(wdir)

if not options.ope:
    print "Modalità non operativa"
    if options.get_meteo:
        print "Scarico i dati storici meteo"
        meteo.retrievehistdata()
        if not options.hist:
            print "Scarico i dati meteo"
            meteo.retrievedata()
    if options.get_pollini:
        print "Scarico i dati storici pollini"
        pollini.retrievehistdata(prov_string, lon_string, lat_string, provfile_string)
        if not options.hist:
            print "Scarico i dati pollini"
            pollini.retrievedata(prov_string, lon_string, lat_string, provfile_string)
#    if options.create_template:
#        print "Creo i template"
#        pollini.create_template()
else:
    print "Modalità operativa"
    print "Lavorerò fino a ",max_time.isoformat(" ")
#    pollini.create_template()
    print "Scarico i dati storici meteo"
    meteo.retrievehistdata()
    print "Scarico i dati storici pollini"
    pollini.retrievehistdata(prov_string, lon_string, lat_string, provfile_string)
    while(True):
# get/refresh data
        meteo.retrievedata()
        pollini.retrievedata(prov_string, lon_string, lat_string, provfile_string)
#        sys.stdout.flush()
#        sys.stderr.flush()
# run something if possible
        run.run_forecast(meteo, pollini)
        time.sleep(600)
# check if any run has finished
        if run.check_forecast(meteo, options.no_transfer):
            print "Tutto completato"
            break
# exit when too late
        if datetime.datetime.now() > max_time:
            print "Tempo scaduto, non tutto completato"
            break
