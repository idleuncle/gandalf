#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys
import time
import re
import lmdb
import msgpack
import numpy as np
import pandas as pd
import jieba.posseg as pseg

# original 保存原始文本
# words 保存

class Corpus:
    def __init__(self, corpusPath, open=True):
        self.LMDB_MAP_SIZE = 1024 * 1024 * 1024
        self.db = None
        self.corpusPath = corpusPath
        if open:
            self.Open(self.corpusPath)

    def Open(self, corpusPath=None):
        if corpusPath is None:
            corpusPath = self.corpusPath
        else:
            self.corpusPath = corpusPath
        self.db = lmdb.open(corpusPath, self.LMDB_MAP_SIZE)
        return not self.db is None

    def Close(self):
        self.db = None

    def IsOpened(self):
        return not self.db is None

    def GetMaxID(self):
        if self.db is None:
            return
        txn = self.db.begin()
        maxID = txn.get('__MaxID')
        if maxID is None:
            maxID = 0
        return int(maxID)

    def _ChangeMaxID(self, txn, maxID):
        txn.put('__MaxID', str(maxID))

    def _printSentence(self, s):
        strS = ""
        for w in s:
            strW = w[0] + "/" + w[1]
            strS += strW + " "

        print strS


    def ImportExcelFile(self, excelFileName):
        t0 = time.time()

        df = pd.read_excel(excelFileName)
        df.title.fillna('', inplace=True)
        df.content.fillna('', inplace=True)
        df.text = df.title + "\n" + df.content

        data = np.empty((1,0))
        data = np.append(data, df.text)
        numSentences = data.size

        t1 = time.time()
        print "Load excel file %s cost %d seconds" % (excelFileName, t1 - t0)

        print "Start word segment."
        sentences = []
        num = 0
        for d in data:
            s = []
            words = pseg.cut(d)
            for w in words:
                if w.flag == 'x':
                    if w != r'，' or w != r'。':
                        continue
                #if not w.flag in ['n', 'nz', 'v', 'm', 'an']:
                    #continue
                #print w.word + "/" + w.flag
                s.append((w.word, w.flag))

            #print "--------" + str(num) + "--------"
            #self._printSentence(s)
            if num % 100 == 0:
                print "-------- word segment sentences " + str(num) + "/" + str(numSentences) + "--------"
                txn.commit()

            sentences.append(s)

            num += 1

        print "-------- word segment sentences " + str(numSentences) + "/" + str(numSentences) + "--------"
        t2 = time.time()
        print "Word segment done. cost %d seconds" % (t2 - t1)

        # Save
        print "Start save corpus."
        num = 0
        txn = self.db.begin(write=True)
        maxID = self.GetMaxID()
        for s in sentences:
            writeBuf = msgpack.packb(s)
            ID = maxID
            txn.put(str(ID), writeBuf)
            maxID += 1
            if num % 1000 == 0:
                print "-------- save sentences " + str(num) + "/" + str(numSentences) + "--------"
            num += 1

        self._ChangeMaxID(txn, maxID)
        txn.commit()

        print "-------- save sentences " + str(numSentences) + "/" + str(numSentences) + "--------"
        t3 = time.time()
        print "Save corpus done. cost %d seconds" % (t3 - t2)

        print "Total time: %ds" % (t3 - t0)

    def _importPlainFile(self, txn, ID, fileName):
        with open(fileName, 'r') as f:
            content = f.read()
            writeBuf = msgpack.dumps(content)
            txn.put(str(ID), writeBuf)

    def ImportPlainFile(self, fileName):
        txn = self.db.begin(write=True)

        maxID = self.GetMaxID()
        ID = maxID
        maxID += 1

        self._importPlainFile(txn, ID, fileName)

        self._ChangeMaxID(txn, maxID)
        txn.commit()

    def ImportPlainFiles(self, filesRootPath, filePattern=None):
        if self.db is None:
            return
        #reobj = None
        #if not filePattern is None:
            #reobj = re.compile(filePattern)

        files = os.listdir(filesRootPath)
        numSentences = len(files)
        print "numSentences=%d" % (numSentences)
        txn = self.db.begin(write=True)
        maxID = self.GetMaxID()
        #sentences = []
        num = 0
        for fileName in files:
            fullPath = os.path.join(filesRootPath, fileName)
            if os.path.isfile(fullPath):
                #if not reobj is None:
                    #if reobj.match(fileName) is None:
                        #continue
                #print fullPath
                ID = maxID
                maxID += 1

                f = open(fullPath, 'r')
                content = f.read()
                text = fileName + "\n" + content

                s = []
                words = pseg.cut(text)
                for w in words:
                    if w.flag == 'x':
                        if w != r'，' or w != r'。':
                            continue
                    s.append((w.word, w.flag))


                writeBuf = msgpack.packb(s)
                ID = maxID
                txn.put(str(ID), writeBuf)

                if num % 100 == 0:
                    txn.commit()
                    txn = self.db.begin(write=True)
                    print "-------- word segment sentences " + str(num) + "/" + str(numSentences) + "--------"
                num += 1

                #sentences.append(s)


        # Save
        #print "Start save corpus."
        #num = 0
        #txn = self.db.begin(write=True)
        #maxID = self.GetMaxID()
        #for s in sentences:
            #writeBuf = msgpack.packb(s)
            #ID = maxID
            #txn.put(str(ID), writeBuf)
            #maxID += 1
            #if num % 1000 == 0:
                #print "-------- save sentences " + str(num) + "/" + str(numSentences) + "--------"
            #num += 1

        #self._ChangeMaxID(txn, maxID)
        #txn.commit()

        print "-------- save sentences " + str(numSentences) + "/" + str(numSentences) + "--------"
                #self._importPlainFile(txn, ID, fullPath)

        self._ChangeMaxID(txn, maxID)
        txn.commit()

    def QueryText(self, ID):
        if self.db is None:
            return
        txn = self.db.begin()
        return txn.get(str(ID))

    def List(self, lines=-1):
        if not self.IsOpened():
            print "Corpus is not opened."
            return

        num = 0
        txn = self.db.begin()
        for key, datum in txn.cursor():
            if lines > 0 and num > lines:
                break
            s = msgpack.unpackb(datum)
            print "--------" + key + "--------"
            self._printSentence(s)
            num += 1

    def Export(self, fileName, lines=-1):
        if not self.IsOpened():
            print "Corpus is not opened."
            return

        with open(fileName, 'w+') as f:
            t0 = time.time()

            num = 0
            txn = self.db.begin()
            for key, datum in txn.cursor():
                if lines > 0 and num > lines:
                    break;
                if key == '__MaxID':
                    continue
                s = msgpack.unpackb(datum)
                print "--------" + key + "--------"
                buf = ""
                for w in s:
                    buf += w[0] + " "
                buf += ".\n.\n"
                f.write(buf)
                num += 1
            f.close()

        print "Export done. %ds" % (time.time() - t0)

def testLMDB():
    corpusPath="test"
    env = lmdb.open(corpusPath)
    txn = env.begin(write=True)

    txn.put(str(1), "Alice一")
    txn.put(str(2), "Bob二")
    txn.put(str(3), "Peter三")

    txn.delete(str(1))

    txn.put(str(3), "Mark四")

    txn.commit()

    txn = env.begin()
    print txn.get(str(2)).decode('utf-8')

    for key, value in txn.cursor():
        print(key)
        print(value.decode('utf-8'))

def testMsgpack():
    corpus = Corpus('corpus')
    maxID = corpus.GetMaxID()
    print("MaxID=", str(maxID))
    print("Query 2:", corpus.QueryText(0))

    #corpus.ImportFiles('gandalf', '\w+.py$')

def test():
    numArgs = len(sys.argv)
    if numArgs > 2:
        corpusName = sys.argv[1]
        excelFileName = sys.argv[2]
        print corpusName, excelFileName
        corpus = Corpus(corpusName)

        #corpus.ImportPlainFiles(excelFileName, "\w+.txt$")

        #corpus.ImportExcelFile(excelFileName)

        #corpus.List(10)
        corpus.Export('bid.txt', -1)

    else:
        print "Usage: %s <corpus_name>" % sys.argv[0]

def main():
    test()

if __name__ == '__main__':
    main()
