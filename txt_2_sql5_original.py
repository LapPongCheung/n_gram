# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 11:41:32 2017

@author: davidling
"""


import re
import os
import sqlite3
import time
import codecs


def create_sql_tables(path_dbsql):

    '''
    _______________ __________________________________________ ________________________  __________________
    |    t_word   | |             t_3gram                    | |          t_2gram      | |      t_1gram    |
    |------------ | |----------------------------------------| |-----------------------| |-----------------|
    |rowid|word_co| |rowid|ngram_1|ngram_2|ngram_3| ngram_fre| |rowid|ngram_1|ngram_fre| |ngram_1|ngram_fre|
    |    1|apple  | |    1|100    |101    |50     | 10       | |    1|100    | 200     | |    100| 100     |
    |    2|orange | |    2|111    |101    |55     | 1        | |    2|111    | 45      | |    101| 63      |
    |_____________| |_____________________________|__________| |_______________________| |_________________|
    
    
    '''
    conn = sqlite3.connect(path_dbsql)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS t_word (
                word_co VARCHAR(70) PRIMARY KEY NOT NULL)''')
    
    cur.execute('''CREATE TABLE IF NOT EXISTS t_3gram (
                 ngram_1 INTEGER NOT NULL, 
                 ngram_2 INTEGER NOT NULL, 
                 ngram_3 INTEGER NOT NULL,
                 ngram_fre INTEGER NOT NULL,
                 PRIMARY KEY (ngram_1, ngram_2, ngram_3))''')
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS t_2gram (
                 ngram_1 INTEGER NOT NULL, 
                 ngram_2 INTEGER NOT NULL, 
                 ngram_fre INTEGER NOT NULL,
                 PRIMARY KEY (ngram_1, ngram_2))''')
    conn.commit()
    cur.execute('''CREATE TABLE IF NOT EXISTS t_1gram (
                 ngram_1 INTEGER NOT NULL, 
                 ngram_fre INTEGER NOT NULL,
                 PRIMARY KEY (ngram_1))''')
    
    conn.commit()
    cur.close()

def load_t_word(cur):
    print ("loading t_word")
    time_s = time.time()
    
    cur.execute('''SELECT rowid, word_co FROM t_word''')
    m = cur.fetchall()
    t_word = { row[1]:row[0] for row in m}
    
    print  ("loaded %d t_words in %d sec"%(len(t_word.keys()),time.time()-time_s))
    
    if (len(t_word.keys())==0): t_word = {}
    
    return t_word
    
    
def gen_sqlstr(nn, exist, value = 0):
    #INSERT OR IGNORE INTO t_3gram (ngram_1, ngram_2, ngram_3, ngram_fre) VALUES (?,?,?,?)
    if exist == False:
        tables = ['ngram_%d'%n for n in range(1, nn+1)]
        placeholdings = ','.join(['?' for _ in range(nn)])

        return "INSERT OR IGNORE INTO t_%dgram ("%nn +" ,".join(tables) +", ngram_fre) VALUES (" + placeholdings +",?) "
    else:
        tables = ['ngram_%d'%n for n in range(1, nn+1)]

        return "UPDATE t_%dgram SET ngram_fre = ngram_fre + %d WHERE " %(nn, value) + ' and '.join([table + '= %d' for table in tables])

def check_sqlstr(nn):
    tables = ['ngram_%d' % n for n in range(1, nn + 1)]
    return "SELECT ngram_fre FROM t_%dgram WHERE " %nn + ' and '.join([table + '= %d' for table in tables])


if __name__ == '__main__':

    nn = 4
    path_of_corpus_1 = os.path.join('web_5gram', 'data', '1gms')
    path_of_corpus_2 = os.path.join('web_5gram', 'data', '2gms', 'extracted')
    path_of_corpus_3 = os.path.join('web_5gram', 'data', '3gms')
    path_of_corpus_list = [path_of_corpus_1, path_of_corpus_2, path_of_corpus_3]
    path_dbsql = "debug.db"




    #CREATE SQL TABLES
    create_sql_tables(path_dbsql)

    #CONNECT TO SQL
    conn = sqlite3.connect(path_dbsql)
    cur = conn.cursor()

    #LOAD WORD INDEX
    t_word = load_t_word(cur) #dictionary of word index
    wid = len(t_word.keys())+1 #next word index

    for i in range(3, nn):
        path_of_corpus = path_of_corpus_list[i - 1]
        fnames = os.listdir(path_of_corpus)
        time_0 = time.time()
        match_str = ''.join(['(\S+)\s' for k in range(i - 1)] + ['(\S+)\\t(\d+)'])

        for fname in fnames:
            time_s = time.time()

            #READ FILE
            fngram = codecs.open(os.path.join(path_of_corpus, fname), 'r', encoding='utf-8')
            fstring = fngram.read().split('\n')
            if fstring[-1]=='': fstring = fstring[:-1]
            fngram.close()

            #FOR EACH NGRAM
            #0. insert new word
            #1. get word id
            #2. insert new ngram
            #3. export to t_word to txt


            wid_temp = wid
            exist = False
            for line in fstring:
                #read ngram and freq
                if len(re.findall('\d+', line)) >= 2:
                    continue
                lower_line = line.lower()
                m = re.match(match_str, lower_line) #match_str = r'(\S+) (\S+) (\S+) (\d+)'

                if m:
                    ngram, nfreq = [m.group(n) for n in range(1,i+1)], m.group(i+1)

                    #0 insert new word
                    for word in ngram:
                        if not word in t_word:
                            t_word[word] = wid
                            cur.execute("INSERT INTO t_word (rowid, word_co) VALUES (?,?) ",(t_word[word], word))

                            wid+=1


                    #1 get word_id
                    wids = [t_word[word] for word in ngram]

                    #2 insert new ngram
                    cur.execute(check_sqlstr(i) % tuple(wids))
                    value = cur.fetchall()
                    if value == []:
                        cur.execute(gen_sqlstr(i, False), wids+[nfreq])
                        #INSERT OR IGNORE INTO t_3gram (ngram_1, ngram_2, ngram_3, ngram_fre) VALUES (?,?,?,?)
                    else:
                        cur.execute(gen_sqlstr(i, True, int(nfreq)) % tuple(wids))


            print ('finish inserting file: ' + str(fname))

            conn.commit()
        print ('number of new words: ', wid-wid_temp)
        print (time.time()-time_s)


    #cur.execute("SELECT * FROM t_2gram")
    #m=cur.fetchall()
    #for row in m:
    #    print (row)

