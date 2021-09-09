import re
import sys
import os
import collections
import json
import time
import array
import snappy
import traceback
import PorterStemmer

# Global variables
docno_to_id = collections.defaultdict(int)
invidx = collections.defaultdict(list)
docno = collections.defaultdict()
stopwords = []
cf = 1

# Reading the index file when no compression has been used
def c0read(idx, index):
    global invidx
    with open(idx, 'rb') as f:
        f.seek(0,2)
        fsize = f.tell()
        f.seek(0,0)
        ind = 0
        while(fsize):
            size = int.from_bytes(f.read(4), byteorder ='big') # read size of postings list
            posting = []
            fsize -= (4+size)
            while(size):
                docid = int.from_bytes(f.read(4), byteorder ='big')
                posting.append(docid)
                size -= 4
            invidx[index[ind]] = posting
            ind += 1
    return


# Reading the index file when c1 compression has been used
def c1read(idx, index):
    global invidx
    with open(idx, 'rb') as f:
        f.seek(0,2)
        fsize = f.tell()
        f.seek(0,0)
        ind = 0
        while(fsize):
            size = int.from_bytes(f.read(1), byteorder ='big') # read size of d
            fsize -= 1
            data = array.array('B', f.read(size))
            fsize -= size
            n = int(''.join(["{0:08b}".format(d) for d in data]),2) #size of postings list
            posting = []
            i = 0
            while(i<n):
                b = ''
                b1 = ''
                data = array.array('B')
                data.fromfile(f,1)
                b = "{0:08b}".format(data[0])
                b1 += b[1:]
                fsize -= 1
                while (b[0] != '0'):      # we have the lsb 
                    data = array.array('B')
                    data.fromfile(f,1)
                    b = "{0:08b}".format(data[0])
                    b1 += b[1:]
                    fsize -= 1
                    
                gap = int(b1,2)
                if i == 0:
                    posting.append(gap)
                else:
                    posting.append(gap + posting[i-1])
                i += 1
            
            invidx[index[ind]] = posting
            ind += 1
    return

# Reading the index file when c2 compression has been used
def c2read(idx, index):
    global invidx
    with open(idx, 'rb') as f:
        f.seek(0,2)
        fsize = f.tell()
        f.seek(0,0)
        ind = 0
        while(fsize):
            size = int.from_bytes(f.read(1), byteorder ='big') # read size of d
            fsize -= 1
            data = array.array('B', f.read(size))
            fsize -= size
            n = int(''.join(["{0:08b}".format(d) for d in data]),2) #size of postings list
            posting = []
            i = 0
            while(i < n):
                data = array.array('B')
                data.fromfile(f,1)
                fsize -= 1
                b1 = str("{0:b}".format(data[0]))
                if (b1 == '0'):
                    b2 = '1'
                else:
                    while (len(b1.split('0',1)) == 1):     # 0 hasn't been read till now # unary rep not complete
                        data = array.array('B')
                        data.fromfile(f,1)
                        fsize -= 1
                        b1 += str("{0:08b}".format(data[0]))

                    b1 = b1.split('0',1)
                    ll = len(b1[0]) + 1      #next bits to read

                    while (len(b1[1]) < ll-1):    #read next byte from file
                        data = array.array('B')
                        data.fromfile(f,1)
                        fsize -= 1
                        b1[1] += str("{0:08b}".format(data[0]))

                    b3 = '1' + b1[1][:ll-1]
                    b2 = b1[1][ll-1:]
                    l = int(b3,2)
                    while (len(b2) < l-1):
                        data = array.array('B')
                        data.fromfile(f,1)
                        fsize -= 1
                        b2 += str("{0:08b}".format(data[0]))
                    b2 = '1' + b2
                    
                gap = int(b2,2)
                if i == 0:
                    posting.append(gap)
                else:
                    posting.append(gap + posting[i-1])
                i += 1
            
            invidx[index[ind]] = posting
            ind += 1
    return
                    
# Reading the index file when c3 compression has been used
def c3read(idx, index):
    global invidx
    with open(idx, 'rb') as f:
        f.seek(0,2)
        fsize = f.tell()
        f.seek(0,0)
        ind = 0
        while(fsize):
            size = int.from_bytes(f.read(1), byteorder ='big') # read size of d
            fsize -= 1
            data = array.array('B', f.read(size))
            fsize -= size
            n = int(''.join(["{0:08b}".format(d) for d in data]),2)
            posting = snappy.uncompress(f.read(n))
            fsize -= n
            posting = array.array('L', posting)
            p = [posting[0]]
            [p.append(posting[i]+p[i-1]) for i in range(1,len(posting),1)]
            invidx[index[ind]] = p
            ind += 1
    return


# Loading the inverted index into memory
def load_invidx(idx, dic):
    global invidx, docno, docno_to_id, cf, stopwords
    with open(dic, 'r') as f:
        lines = f.readlines()
        index = json.loads(lines[0].strip())
        docno_to_id = json.loads(lines[1].strip())
        cf = lines[2].strip()
        stopwords = json.loads(lines[3].strip())
    
    docno = {v:k for (k,v) in docno_to_id.items()}
    
    if cf == '0':
        c0read(idx, index)
    
    elif cf == '1':
        c1read(idx, index)
    
    elif cf == '2':
        c2read(idx,index)
        
    elif cf == '3':
        c3read(idx,index)

    return


# Computes the intersection of 2 postings list
def pintersection(keywords):
    posting = []
    for i,key in enumerate(keywords):
        if key in invidx.keys():
            if not i:
                posting = invidx[key]
                continue
            posting = set(posting).intersection(invidx[key])
        else:
            return []
    return posting

# Reads the query file and generates the result file
def query(qfile, rfile):
    with open(qfile, 'r') as t, open(rfile, 'w') as r:
        queries = (line for line in t.readlines())
        for i,q in enumerate(queries):
            keywords = q.strip().lower()
            keywords = re.split(r';|,|\'|\"|:|\`|\n|\.|\(|\)|\{|\}|\[|\]| ', keywords)
            keywords = set(filter(None, keywords))
            stemmer = PorterStemmer.PorterStemmer()
            keywords = [k for k in keywords if k not in stopwords]
            keywords = [stemmer.stem(k,0,len(k)-1) for k in keywords]
            posting = pintersection(keywords)
            for docid in posting:
                r.write('Q' + str(i+1) + ' ' + docno[docid]
                        + ' ' + '1.0\n')
    return

def main():
    try:
        queryfile = sys.argv[1]
        resultfile = sys.argv[2]
        indexfile = sys.argv[3]
        dictionary = sys.argv[4]
        if (os.path.splitext(indexfile)[1] != '.idx'):
            indexfile += '.idx'
        if (os.path.splitext(dictionary)[1] != '.dict'):
            dictionary += '.dict'
        load_invidx(indexfile, dictionary)
        query(queryfile, resultfile)

    except Exception as e:
        print('ERROR: ',e)
        traceback.print_exc()
    return

if __name__ == '__main__':
    main()
