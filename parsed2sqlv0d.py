import sqlite3
import time
import bz2
import sys
import re
from itertools import islice
import apsw

# dependency and part of speech mapping
dep_t2n = {"acomp": 1, "advcl": 2, "advmod": 3, "agent": 4, "amod": 5, "appos": 6, "attr": 7, "aux": 8, "auxpass": 9,
           "cc": 10, "ccomp": 11, "complm": 12, "conj": 13, "cop": 14, "csubj": 15, "csubjpass": 16, "dep": 17,
           "det": 18, "dobj": 19, "expl": 20, "hmod": 21, "hyph": 22, "infmod": 23, "intj": 24, "iobj": 25, "mark": 26,
           "meta": 27, "neg": 28, "nmod": 29, "nn": 30, "npadvmod": 31, "nsubj": 32, "nsubjpass": 33, "num": 34,
           "number": 35, "oprd": 36, "obj": 37, "obl": 38, "parataxis": 39, "partmod": 40, "pcomp": 41, "pobj": 42,
           "poss": 43, "possessive": 44, "preconj": 45, "prep": 46, "prt": 47, "punct": 48, "quantmod": 49, "relcl": 50,
           "root": 51, "xcomp": 52, "acl": 53, "case": 54, "compound": 55, "dative": 56, "nummod": 57, "predet": 58}
pos_t2n = {"-LRB-": 1, "-RRB-": 2, ",": 3, ":": 4, ".": 5, "''": 6, '""': 7, "#": 8, "``": 9, "$": 10, "ADD": 11,
           "AFX": 12, "BES": 13, "CC": 14, "CD": 15, "DT": 16, "EX": 17, "FW": 18, "GW": 19, "HVS": 20, "HYPH": 21,
           "IN": 22, "JJ": 23, "JJR": 24, "JJS": 25, "LS": 26, "MD": 27, "NFP": 28, "NIL": 29, "NN": 30, "NNP": 31,
           "NNPS": 32, "NNS": 33, "PDT": 34, "POS": 35, "PRP": 36, "PRP$": 37, "RB": 38, "RBR": 39, "RBS": 40, "RP": 41,
           "_SP": 42, "SYM": 43, "TO": 44, "UH": 45, "VB": 46, "VBD": 47, "VBG": 48, "VBN": 49, "VBP": 50, "VBZ": 51,
           "WDT": 52, "WP": 53, "WP$": 54, "WRB": 55, "XX": 56}
dep_n2t = {dep_t2n[key]: key for key in dep_t2n}
pos_n2t = {pos_t2n[key]: key for key in pos_t2n}


def create_sql_tables(conn):
    '''
    ver 0
    _______________ ________________________________________________________
    |    t_word   | |                         t_5gram                       | 
    |------------ | |-------------------------------------------------------| 
    |rowid|word_co| |rowid|w1     |w1_pos |dep_tag| w2    |w2_pos| ngram_fre|
    |    1|apple  | |    1|100    |1      |25     | 101   |2     | 3999     |
    |    2|orange | |    2|111    |10     |51     | 122   |5     | 2010     |
    |_____________| |_______________________________________________________|
    Notes: ver0: w1 w1_pos dep_tag w2 w2_pos are primary keys

'''

    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS t_word (
                word_co VARCHAR(70) PRIMARY KEY NOT NULL)''')
    # ver 0
    cur.execute('''CREATE TABLE IF NOT EXISTS t_5gram (
                 w1 INTEGER NOT NULL, 
                 w1_pos INTEGER NOT NULL, 
                 dep_tag INTEGER NOT NULL,
                 w2 INTEGER NOT NULL, 
                 w2_pos INTEGER NOT NULL, 
                 ngram_fre INTEGER NOT NULL,
                 PRIMARY KEY (w1,w1_pos,dep_tag,w2,w2_pos))''')
    cur.close()


def get_wid(word, conn):
    cur = conn.cursor()
    cur.execute("SELECT rowid FROM t_word WHERE word_co =?", (word,))
    m = cur.fetchone()

    if not m:
        cur.execute("INSERT INTO t_word (word_co) VALUES (?)", (word,))
        cur.execute("SELECT rowid FROM t_word WHERE word_co =?", (word,))
        m = cur.fetchone()

    cur.close()
    return m[0]


def load_n_insert(f_path, conn):
    print("Start loading and inserting... %s" % (f_path,))
    cur = conn.cursor()
    fdata = bz2.open(f_path, "r")

    counter, ctime0, dtime0 = 0, time.time(), time.time()
    for line in fdata:
        data = line.decode('utf-8')
        m = re.match(r'(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\n', data)

        if m:
            w1, pos1, dep, w2, pos2, freq = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), int(m.group(6))

            # turn into numbers
            w1_n, w2_n = get_wid(w1, conn), get_wid(w2, conn)
            dep_n = dep_t2n[dep]
            pos1_n, pos2_n = pos_t2n[pos1], pos_t2n[pos2]

            # check existing records
            cur.execute("SELECT ngram_fre from t_5gram WHERE w1=? AND w1_pos =? AND dep_tag=? AND w2=? AND w2_pos=?",
                        (w1_n, pos1_n, dep_n, w2_n, pos2_n))
            existing = cur.fetchone()
            if existing:
                freq += existing[0]
                cur.execute(
                    "UPDATE t_5gram SET ngram_fre = ? WHERE w1=? AND w1_pos =? AND dep_tag=? AND w2=? AND w2_pos=?",
                    (freq, w1_n, pos1_n, dep_n, w2_n, pos2_n))
            else:
                cur.execute("INSERT INTO t_5gram  (w1,w1_pos,dep_tag,w2,w2_pos,ngram_fre) VALUES (?,?,?,?,?,?) ",
                            (w1_n, pos1_n, dep_n, w2_n, pos2_n, freq))

            counter += 1
            if (counter % 1500000 == 0):
                print("Done %d lines, dtime = %d sec, ctime = %d sec" % (
                counter, time.time() - dtime0, time.time() - ctime0))
                dtime0 = time.time()
    print("Done %s, ctime = %d sec" % (f_path, time.time() - ctime0))


path_dbsql = r"C:\Users\lpcheung\Documents\hsmc\n_gram/dep_parser_wiki18_v0d.db"
path_parsed_folder = r"C:/Users/davidling/Documents/Python Scripts/eng/parser/parsed_wiki18/"
parsed_fnames = ['wiki_0-32.txt.bz2', 'wiki_32-64.txt.bz2', 'wiki_64-96.txt.bz2', 'wiki_96-127.txt.bz2']

# path_dbsql = r"/data/davidling/parser/dep_parser_wiki18_v0d.db"
# path_parsed_folder = r"/data/davidling/parser/"
# parsed_fnames = ['wiki_0-32.txt.bz2','wiki_32-64.txt.bz2']


###CREAT IN-MEMORY SQL DATABASE
conn = apsw.Connection(':memory:')
create_sql_tables(conn)

###LOAD AND INSERT TO IN-MEMORY SQL
for fname in parsed_fnames:
    load_n_insert(path_parsed_folder + fname, conn)

###EXPORTING TO DISK
time0 = time.time()
print('Exporting to disk...')
diskcon = apsw.Connection(path_dbsql)
with diskcon.backup("main", conn, "main") as backup:
    backup.step()
print('Done exporting to disk, dtime = %d sec' % (time.time() - time0))

conn.close()
