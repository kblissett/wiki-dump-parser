#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import time
import json
import re

STRIP=r' \([^)]*\)|,' # remove () and ,

def load_typed_titles(path):
    res = dict()
    for line in open(path):
        tmp = line.strip().split('\t')
        kbid = tmp[0]
        trans = tmp[1]
        etype = tmp[2]
        res[kbid] = etype
    return res

def typing(p_tt, p_data, outdir):
    typed_titles = load_typed_titles(p_tt)
    res = dict()
    outlinks = json.load(open(p_data))
    count = 0
    for ol in outlinks:
        for kbid in outlinks[ol]:
            if kbid == '#TOTAL#':
                continue
            try:
                etype = typed_titles[kbid.encode('utf-8')]
            except:
                print 'Can find kbid: %s' % kbid
                continue
            if etype == 'NIL':
                continue
            if ol not in res:
                res[ol] = dict()
            res[ol][kbid] = (etype, outlinks[ol][kbid])
            count += 1
            # if count % 10000 == 0:
            #     print '10000 kbids parsed'
    out = open('%s/%s' % (outdir, 'typed_outlinks.json'), 'w')
    out.write(json.dumps(res))

def count(p_tt, p_data, outdir):
    typed_titles = load_typed_titles(p_tt)
    res = dict()
    outlinks = json.load(open(p_data))
    count = 0
    for ol in outlinks:
        for kbid in outlinks[ol]:
            if kbid == '#TOTAL#':
                continue
            try:
                etype = typed_titles[kbid.encode('utf-8')]
            except:
                print 'Can find kbid: %s' % kbid
                continue
            if etype == 'NIL':
                continue
            if etype == 'MISC':
                continue

            toks = filter(None, re.sub(STRIP, '', ol).split(' '))
            for t in toks:
                if len(toks) == 1:
                    tag = '%s-U' % etype
                elif toks.index(t) == 0:
                    tag = '%s-B' % etype
                else:
                    tag = '%s-I' % etype
                if t not in res:
                    res[t] = dict()
                if tag not in res[t]:
                    res[t][tag] = 0
                res[t][tag] += outlinks[ol][kbid]
    out = open('%s/%s' % (outdir, 'words.json'), 'w')
    out.write(json.dumps(res))

if __name__ == '__main__':
    s = time.time()

    if len(sys.argv) != 4:
        print 'USAGE: get_commonness.py ' \
            '<path to typed title> <path to commonness.json> <outdir>'
        sys.exit()

    p_tt = sys.argv[1]
    p_data = sys.argv[2]
    outdir = sys.argv[3]
    typing(p_tt, p_data, outdir)
    count(p_tt, p_data, outdir)

    print time.time() - s
