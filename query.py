import time
import sqlite3

query = "SELECT ngram_1, ngram_2, ngram_3 FROM t_3gram WHERE ngram_1=1 AND ngram_2=2 AND ngram_3=3;"
path_dbsql = "gngramv1.db"

db = sqlite3.connect(path_dbsql)
cur = db.cursor()
time = time.time()
cur.execute(query)
print (time.time() - time)
db.close()