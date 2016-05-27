#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import time
sys.path.append('/data/m1/panx2/code/kbid2type')
import kbid2type
sys.path.append('/data/m1/panx2/code/wiki-langlink-parser/search')
import langlink_table_all2en

def typing(p_titles, outdir):
    count = dict()
    count['etype'] = 0
    res = list()
    for line in open(p_titles):
        kbid = line.strip()
        tres = kbid2type.typing(kbid.replace(' ', '_'))
        if tres[0][1] == tres[-1][1]:
            etype = 'NIL'
        else:
            etype = tres[0][0]
        res.append((kbid, etype, tres))
    out = open('%s/%s' % (outdir, 'typed_titles'), 'w')
    for i in res:
        out.write('%s\t%s\t%s\n' % (i[0], i[1], i[2]))
    print count['etype']
    print len(res)

def typing(p_titles, outdir, lang):
    count = dict()
    count['etype'] = 0
    count['trans'] = 0
    res = list()
    for line in open(p_titles):
        kbid = line.strip()
        trans = langlink_table_all2en.search(kbid.replace('_', ' '), lang=lang)
        if trans:
            count['trans'] += 1
            transed_kbid = trans[lang]
            tres = kbid2type.typing(transed_kbid.replace(' ', '_'))
            if tres[0][1] != tres[-1][1]:
                count['etype'] += 1
                etype = tres[0][0]
            else:
                etype = 'NIL'
        else:
            transed_kbid = None
            etype = 'NIL'
            tres = list()
        res.append((kbid, transed_kbid, etype, tres))
        if len(res) % 10000 == 0:
            print '10000 kbids have been typed'
    out = open('%s/%s' % (outdir, 'typed_titles'), 'w')
    for i in res:
        out.write('%s\t%s\t%s\t%s\n' % (i[0], i[1], i[2], i[3]))
    print count['trans']
    print count['etype']
    print len(res)

if __name__ == '__main__':
    s = time.time()

    if len(sys.argv) != 4:
        print 'USAGE: get_commonness.py ' \
            '<path to title flat> <outdir> <lang>'
        sys.exit()

    path = sys.argv[1]
    outdir = sys.argv[2]
    lang = sys.argv[3]
    typing(path, outdir, lang)

    print time.time() - s
