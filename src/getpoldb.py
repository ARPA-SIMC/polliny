#!/usr/bin/python3

import datetime
import sys, optparse
#import ArpaeSecrets

# prototipo di estrazione da pds a pde con scrittura in outfile
def getpolldb(pds, pde, outfile):
    outfile.write(str(pds)+" "+str(pde)+"\n")


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
        de = datetime.datetime(*time.strptime(options.enddate, "%Y-%m-%d")[0:3])
    else:
        de = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if options.startdate is not None:
        ds = datetime.datetime(*time.strptime(options.startdate, "%Y-%m-%d")[0:3])
    else:
        ds = de - datetime.timedelta(days=7)
    if options.outfile is not None:
        of = open(options.outfile, mode="w")
    else:
        of = sys.stdout

    getpolldb(ds, de, of)
