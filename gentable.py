def gen_table(match_str, insert_str, cur, path_folder_ngram_txt, path_dbsql):


    t_word = load_t_word(cur)  # dictionary of word index
    wid = len(t_word.keys()) + 1  # next word index

    fnames = os.listdir(path_folder_ngram_txt)
    fnames = os.listdir(path_folder_ngram_txt)
    time_0 = time.time()

    for fname in fnames:
        time_s = time.time()
        file_size = os.stat(os.path.join(path_folder_ngram_txt, fname)).st_size
        print(fname, ' ', file_size)

        # READ FILE
        fngram = codecs.open(os.path.join(path_folder_ngram_txt, fname), 'r', encoding='utf-8')
        fstring = fngram.read().split('\n')  # why?
        if fstring[-1] == '': fstring = fstring[:-1]
        fngram.close()

        # FOR EACH NGRAM
        # 0. insert new word
        # 1. get word id
        # 2. insert new ngram
        # 3. export to t_word to txt

        wid_temp = wid
        i = 0

        print("going to for loop")
        for line in fstring:
            # read ngram and freq
            if len(re.findall('\d+', line)) >= 2:
                continue

            m = re.match(match_str, line)  # match_str = r'(\S+) (\S+) (\S+) (\d+)'

            if m:
                ngram, nfreq = [m.group(n) for n in range(1, nn + 1)], m.group(nn + 1)

                # 0 insert new word
                for word in ngram:
                    if not word in t_word:
                        t_word[word] = wid
                        cur.execute("INSERT INTO t_word (rowid, word_co) VALUES (?,?) ", (t_word[word], word))

                        wid += 1

                # 1 get word_id
                wids = [t_word[word] for word in ngram]

                # 2 insert new ngram
                cur.execute(insert_str, wids + [
                    nfreq])  # INSERT OR IGNORE INTO t_3gram (ngram_1, ngram_2, ngram_3, ngram_fre) VALUES (?,?,?,?)

                i += 1

        #        if (i>5): break

        # conn.commit()
        print('number of new words: ', wid - wid_temp)
        print(time.time() - time_s)

        time0 = time.time()
        print('Exporting to disk...')
        diskcon = apsw.Connection(path_dbsql)
        with diskcon.backup("main", conn, "main") as backup:
            backup.step()
    print('Done exporting to disk, dtime = %d sec' % (time.time() - time0))