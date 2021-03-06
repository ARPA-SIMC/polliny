# -*- coding: utf-8 -*-

staz = \
[{'nome': 'Piacenza',
  'intlat': '4506052',
  'intlon': '970554',
  'alt': '55',
  'sigla': 'PC1',
  'idstaz': '4176',
  'gias': '00369'},
 {'nome': 'Parma',
  'intlat': '4478333',
  'intlon': '1031667',
  'alt': '50',
  'sigla': 'PR2',
  'idstaz': '4177',
  'gias': '00774'},
 {'nome': 'Reggio Emilia',
  'intlat': '4465546',
  'intlon': '1059917',
  'alt': '94',
  'sigla': 'RE1',
  'idstaz': '4178',
  'gias': '00977'},
 {'nome': 'Modena',
  'intlat': '4468389',
  'intlon': '1097000',
  'alt': '28',
  'sigla': 'MO1',
  'idstaz': '4179',
  'gias': '01138'},
 {'nome': 'Vignola',
  'intlat': '4450405',
  'intlon': '1100414',
  'alt': '100',
  'sigla': 'MO2',
  'idstaz': '4180',
  'gias': '01221'},
 {'nome': 'Bologna',
  'intlat': '4450122',
  'intlon': '1132820',
  'alt': '80',
  'sigla': 'BO1',
  'idstaz': '4181',
  'gias': '01421'},
 {'nome': 'Ferrara',
  'intlat': '4483333',
  'intlon': '1161667',
  'alt': '8',
  'sigla': 'FE1',
  'idstaz': '4182',
  'gias': '01574'},
 {'nome': 'Faenza',
  'intlat': '4428688',
  'intlon': '1189148',
  'alt': '37',
  'sigla': 'RA2',
  'idstaz': '4184',
  'gias': '01786'},
 {'nome': 'Ravenna',
  'intlat': '4446535',
  'intlon': '1220555',
  'alt': '2',
  'sigla': 'RA3',
  'idstaz': '4185',
  'gias': '01983'},
 {'nome': 'Forlì',
  'intlat': '4422698',
  'intlon': '1202757',
  'alt': '30',
  'sigla': 'FO1',
  'idstaz': '4186',
  'gias': '01867'},
 {'nome': 'Cesena',
  'intlat': '4413971',
  'intlon': '1223505',
  'alt': '31',
  'sigla': 'FO2',
  'idstaz': '4187',
  'gias': '01989'},
 {'nome': 'Rimini',
  'intlat': '4403333',
  'intlon': '1261667',
  'alt': '7',
  'sigla': 'FO3',
  'idstaz': '4188',
  'gias': '02231'},
 {'nome': 'San Giovanni Persiceto',
  'intlat': '4463290',
  'intlon': '1119489',
  'alt': '38',
  'sigla': 'BO3',
  'idstaz': '4189',
  'gias': '01338'},
 {'nome': 'San Pietro Capofiume',
  'intlat': '4465000',
  'intlon': '1161667',
  'alt': '10',
  'sigla': 'BO5',
  'idstaz': '4190',
  'gias': '01617'}]

vars = ['B48031', 'B48002', 'B48003', 'B48023', 'B48066', 'B48005',
        'B48006', 'B48007', 'B48079', 'B48080', 'B48008', 'B48021', 'B48036',
        'B48025', 'B48011', 'B48012', 'B48013', 'B48001', 'B48038', 'B48037',
        'B48026', 'B48081', 'B48082', 'B48016', 'B48015', 'B48017', 'B48032',
        'B48018', 'B48030', 'B48024', 'B48034', 'B48033', 'B48029', 'B48077',
        'B48019', 'B48045', 'B48044', 'B48039', 'B48041']


def prov_to_key(prov, key):
    cod = list(x for x in staz if x['sigla'] == prov)
    if len(cod) > 0: return cod[0][key]
    else: return None

def statid_to_key(statid, key):
    cod = list(x for x in staz if x['idstaz'] == statid)
    if len(cod) > 0: return cod[0][key]
    else: return None

def coord_to_key(intlon, intlat, key):
    cod = list(x for x in staz if x['intlon'] == intlon and x['intlat'] == intlat)
    if len(cod) > 0: return cod[0][key]
    else: return None
