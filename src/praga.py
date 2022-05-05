#!/usr/bin/python

import pandas
import MySQLdb
import ArpaeSecrets

def pragaquery(tabled, tableh, ds, de, ofile):
    conn = MySQLdb.connect(
        user=ArpaeSecrets.pragauser,
        passwd=ArpaeSecrets.pragapasswd,
        host=ArpaeSecrets.pragahost,
        db=ArpaeSecrets.pragadb,
    )

    df1 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
NULL AS ET_H_SMR,
AVG(value) AS VENTO_SMR,
NULL AS VD_SMR,
MAX(value) AS VMAX_SMR
FROM %s
WHERE variablecode = 105
AND pragatime >= "%s" AND pragatime < "%s"
GROUP BY DATE(pragatime)
""" % (tableh,ds,de), conn
    ).set_index("TEMPO")

    df2 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
value AS TEMP_MIN_SMR
FROM %s
WHERE variablecode = 1
AND pragatime >= "%s" AND pragatime < "%s"
""" % (tabled,ds,de), conn
    ).set_index("TEMPO")

    df3 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
value AS TEMP_MEDIA_SMR
FROM %s
WHERE variablecode = 3
AND pragatime >= "%s" AND pragatime < "%s"
""" % (tabled,ds,de), conn
    ).set_index("TEMPO")

    df4 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
value AS TEMP_MAX_SMR
FROM %s
WHERE variablecode = 2
AND pragatime >= "%s" AND pragatime < "%s"
""" % (tabled,ds,de), conn
    ).set_index("TEMPO")

    df5 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
MIN(value) AS UMIN_SMR,
MAX(value) AS UMAX_SMR
FROM %s
WHERE variablecode = 103
AND pragatime >= "%s" AND pragatime < "%s"
GROUP BY DATE(pragatime);
""" % (tableh,ds,de), conn
    ).set_index("TEMPO")

    df6 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
value AS U_R_SMR
FROM %s
WHERE variablecode = 7
AND pragatime >= "%s" AND pragatime < "%s"
""" % (tabled,ds,de), conn
    ).set_index("TEMPO")

    df7 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
value AS PREC_SMR
FROM %s
WHERE variablecode = 4
AND pragatime >= "%s" AND pragatime < "%s"
""" % (tabled,ds,de), conn
    ).set_index("TEMPO")

    df8 = pandas.read_sql(
"""SELECT DATE(pragatime) AS TEMPO,
AVG(value) AS RAD_SMR
FROM %s
WHERE variablecode = 104
AND pragatime >= "%s" AND pragatime < "%s"
GROUP BY DATE(pragatime)
""" % (tableh,ds,de), conn
    ).set_index("TEMPO")

# to complete with data that now are either computed in R or missing
# in Praga but present in forecast data
#    dummy = pandas.DataFrame(
#        {"ET_H_SMR": [], "VD_SMR": []}
#    )

    #df = df1.join([df2, df3, df4, df5, df6, df7, df8])
    df = df1.join([df2, df3, df4, df5, df6, df7, df8], how="left").sort_index()
    df.to_csv(ofile)
# since radiation is not always available, with how="outer" the TEMPO
# header disappears when radiation column is (partly) missing

if __name__ == '__main__':
    import sys
    pragaquery("00100_d", "00100_h",
               "2001-01-01 00:00:00", "2001-01-03 00:00:00",
               sys.stdout)
