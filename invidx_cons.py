import re
import sys
import os
import collections
import json
import time
import PorterStemmer
import bs4
import array
import snappy
import traceback

# Global variables
xml_tags = []
stopwords = []
docno_to_id = collections.defaultdict(int)
invidx = collections.defaultdict(list)
ID = 1

# Read stopwords from file to list
def read_stopword_file(stopwordfile):
    global stopwords
    with open(stopwordfile, 'r') as f:
        lines = (line for line in f.readlines()) 
        stopwords.extend([line.strip() for line in lines])

# Read xml tags from file to list
def read_xml_tags(xmlTags):
    global xml_tags
    with open(xmlTags, 'r') as f:
        lines = (line for line in f.readlines()) 
        next(lines)
        for line in lines:
            xml_tags.append(line.strip().upper())

# Extracts desired tags from each document in the 
# collection and adds the indexes and postings list 
# to the inverted index
def parse_file(content):
    global invidx, stopwords, xml_tags, ID
    for d1 in content:
        try:
            soup = bs4.BeautifulSoup(d1, 'xml').find('DOC')
            docno = str(soup.find('DOCNO').text).strip()
            tags = xml_tags
            text = ''
            for tag in tags:
                t = soup.find(tag)
                if t:
                    text += (t.text.lower() + ' ')
            if text:
                docno_to_id[docno] = ID
                ID += 1
                #tokenize the text using <white-space> and ,.:;"’
                text = re.split(r';|,|\'|\"|:|\`|\n|\.|\(|\)|\{|\}|\[|\]| ',text)
                text = set(filter(None, text))
                text = [t for t in text if t not in stopwords]
                stemmer = PorterStemmer.PorterStemmer()
                text = [stemmer.stem(t,0,len(t)-1) for t in text]
                [invidx[t].append(docno_to_id[docno]) for t in set(text)]
        
        except Exception as e:
            print('ERROR: ', e)
            traceback.print_exc()

    return

# Reads the files in the collection
def read_collection(collPath):
    try:
        for file in os.listdir(collPath):
            if (file == 'ap890520'):
                continue
            with open(collPath + '/' + file, 'r') as f:
                lines = (line.strip() for line in f.readlines()) 
                lines = ' '.join(lines)
                lines = lines.split('</DOC>')[:-1]
                parse_file(lines)
        
    except Exception as e:
        print('ERROR:', e)
        traceback.print_exc()
        
    return


# Generates the encoding for c1 compression
def getencodingc1(n):
    b = str("{0:b}".format(int(n)))[::-1]
    blocks = len(b)//7
    padding = 7 - (len(b) - 7*blocks)
    rep = ''
    i = 0
    while(i < len(b)):
        if (i == 7*blocks):
            rep += b[i:] 
            rep += '0'*padding
            if not blocks:
                rep += '0'
            else:
                rep += '1'
        elif i == 0:
            rep = b[i:i+7]
            rep += '0'
        else:
            rep += b[i:i+7]
            rep += '1'
        i += 7
    i=0
    rep = rep[::-1]
    data = array.array('B', [int(rep[i:i+8],2) for i in range(0,len(rep),8)])
    return data

# Writes the postings list to file using c1 compression
def c1dump(indexfile):
    with open(indexfile + '.idx', 'wb') as f:
        for posting in invidx.values():
            p = [posting[0]]
            p.extend([(posting[i]-posting[i-1]) for i in range(1,len(posting))])
            b = "{0:08b}".format(len(posting))
            n = len(b)
            blocks = n//8 +1
            if (n%8 != 0):
                b = '0'*(8*blocks-n) + b
            binrep = [b[i:i+8] for i in range(0,n,8)]
            d = array.array('B', [int(i,2) for i in binrep])
            f.write(len(d).to_bytes(1, byteorder='big'))
            d.tofile(f)
            [getencodingc1(data).tofile(f) for data in p]
    return


# Generates the encoding for c2 compression
def getencodingc2(n):
    b = "{0:b}".format(int(n))
    l = len(b)
    b1 = "{0:b}".format(l)
    ll = len(b1)
    u = '1'*(ll-1) + '0'
    lsb = b[1:]
    lsb1 = b1[1:]
    rep = u + lsb1 + lsb
    data = array.array('B')
    i = len(rep)
    while (i>8):
        data.append(int(str(rep[i-8:i]),2))
        i -= 8
    if i>0:
        data.append(int(rep[:i],2))
    return data[::-1]


# Writes the postings list to file using c2 compression
def c2dump(indexfile):
    with open(indexfile + '.idx', 'wb') as f:
        for posting in invidx.values():
            p = [posting[0]]
            p.extend([(posting[i]-posting[i-1]) for i in range(1,len(posting))])
            b = "{0:08b}".format(len(p))
            n = len(b)
            blocks = n//8 +1
            if (n%8 != 0):
                b = '0'*(8*blocks-n) + b
            binrep = [b[i:i+8] for i in range(0,n,8)]
            d = array.array('B', [int(i,2) for i in binrep])
            f.write(len(d).to_bytes(1, byteorder='big'))
            d.tofile(f)
            [getencodingc2(data).tofile(f) for data in p]
    return


# Writes the postings list to file using c3 compression
def c3dump(indexfile):
    with open(indexfile + '.idx', 'wb') as f:
        for posting in invidx.values():
            p = [posting[0]]
            p.extend([(posting[i]-posting[i-1]) for i in range(1,len(posting))])
            data = array.array('L', p)
            cstr = snappy.compress(data)
            n = len(cstr)
            b = "{0:08b}".format(n)
            n = len(b)
            blocks = n//8 +1
            if (n%8 != 0):
                b = '0'*(8*blocks-n) + b
            binrep = [b[i:i+8] for i in range(0,n,8)]
            d = array.array('B', [int(i,2) for i in binrep])
            f.write(len(d).to_bytes(1, byteorder='big'))
            d.tofile(f)
            f.write(cstr)
    return

# Writes the index + docno to docid mapping + stopwords list
# + the compression information and postings list to files
def dump_to_file(indexfile,cf):
    global invidx
    with open(indexfile + '.dict', 'w') as index:
        json.dump(list(invidx.keys()), index)
        index.write('\n')
        json.dump(docno_to_id,index)
        index.write('\n')
        index.write(str(cf))
        index.write('\n')
        json.dump(stopwords, index)
    
    if not cf:
        with open(indexfile + '.idx', 'wb') as f:
            for posting in invidx.values():
                n = len(posting)*4
                f.write(n.to_bytes(4,byteorder='big',signed=False))
                [f.write(i.to_bytes(4,byteorder='big',signed=False)) for i in posting]
                    
    elif cf == 1:
        c1dump(indexfile)
        
    elif cf == 2:
        c2dump(indexfile)
    
    elif cf == 3:
        c3dump(indexfile)
                
    return


def main():
    try:
        collPath = sys.argv[1]
        indexfile = sys.argv[2]
        stopwordfile = sys.argv[3]
        cf = int(sys.argv[4])
        xmlTags = sys.argv[5]
        if (cf == 4 or cf == 5):
            print('not implemented')
            return 

        read_xml_tags(xmlTags)
        read_stopword_file(stopwordfile)
        read_collection(collPath)

        # Inverted index created now dump to file
        dump_to_file(indexfile,cf)
        
    except Exception as e:
        print('ERROR: ', e)
        traceback.print_exc()

if __name__ == '__main__':
    main()

        