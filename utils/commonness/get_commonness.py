from __future__ import division
#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import os
import sys
sys.setrecursionlimit(5000)
import time
from whoosh.index import create_in
from whoosh.fields import *
import json

def split_nested_outlinks(text, content):
    if len(text) == 0:
        return
    else:
        if text[0:2] == '[[':
            content.append(list())
            for i in content:
                i.append(text[0:2])
            text = text[2:]
            split_nested_outlinks(text, content)
        elif text[0:2] == ']]':
            for i in content:
                i.append(text[0:2])
            links.append(''.join(content[-1]))
            content.pop(-1)
            text = text[2:]
            split_nested_outlinks(text, content)
        else:
            for i in content:
                i.append(text[0])
            text = text[1:]
            split_nested_outlinks(text, content)

def strip_outlink(link):
    if link.count('[[') > 1 or link.count(']]') > 1 or link.count('|') > 1:
        return '', ''
    if re.match('\[\[\]\]|\[\[\s+\]\]', link):
        return '', ''
    m = re.search('\[\[(.+?)\|(.*)\]\]', link)
    if m != None: # Renamed link
        title = m.group(1).strip(' ').lstrip(' ')
        text = m.group(2).strip(' ').lstrip(' ')
        if re.match('\s+', text) or text == '':
            text = title
    else: # Regular link
        rl  = re.search('\[\[(.+)\]\]', link).group(1).strip(' ').lstrip(' ')
        title = rl
        text = rl
    return title, text

def load_redirect_table(path):
    res = dict()
    for line in open(path, 'r'):
        tmp = line.strip().split('\t')
        res[tmp[1]] = tmp[2]
    return res

def index(commonness, index_dir, procs=1, limitmb=1048, multisegment=False):
    schema = Schema(text=TEXT(stored=True),
                    titles=STORED)

    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
    ix = create_in(index_dir, schema)
    writer = ix.writer(procs=procs, limitmb=limitmb, multisegment=multisegment)
    for text in commonness:
        try:
            titles = '\n'.join(['%s\t%s' % (title, commonness[text][title]) \
                                for title in commonness[text]])
            writer.add_document(text=text.decode('utf-8'),
                                titles=titles.decode('utf-8'))
        except:
            print sys.exc_info()
    writer.commit()

def parse(p_outlink, p_redirect, score=False, outdir=''):
    res = dict()
    err = list()
    rd_table = load_redirect_table(p_redirect)
    pages = dict()


    for line in open(p_outlink, 'r'):
        tmp = line.strip().split('\t')
        pt = tmp[0]
        ol = tmp[1:]
        pages[pt] = ol

    all_titles = set(pages.keys())
    for pt in pages:
        # Count page title once
        text = pt
        if re.match('\s+', text) or text == '':
            continue
        res[text] = dict()
        title = pt
        try:
            title = rd_table[title]
        except:
            pass
        res[text][title] = 1
        res[text]['#TOTAL#'] = 1

        # Count outlinks in this page
        for i in pages[pt]:
            # Parse nested outlink
            global links
            links = list()
            try:
                split_nested_outlinks(i, list())
            except:
                err.append('NESTED: %s' % i)
                continue

            for link in links:
                # Strip link
                try:
                    title, text = strip_outlink(link)
                except:
                    err.append('LINK: %s' % link)
                if text == '' or title == '':
                    continue
                # Revsie title
                if not title[0].isupper():
                    title = title.decode('utf-8')
                    title = title[0].upper() + title[1:]
                    title = title.encode('utf-8')
                try:
                    title = rd_table[title]
                except:
                    pass
                # Title has to be valid
                if title not in all_titles:
                    err.append('TITLE: %s' % title)
                    continue

                if text not in res:
                    res[text] = dict()
                    res[text]['#TOTAL#'] = 0
                if title not in res[text]:
                    res[text][title] = 0
                res[text][title] += 1
                res[text]['#TOTAL#'] += 1

    # Compute Commonness score
    if score:
        for text in res:
            for title in res[text]:
                if title == '#TOTAL#':
                    continue
                res[text][title] = res[text][title] / res[text]['#TOTAL#']

    if outdir:
        try:
            os.mkdir(outdir)
        except:
            pass
        out = open('%s/%s' % (outdir, 'commonness.json'), 'w')
        out.write(json.dumps(res))
        out.close()
        if err:
            err_out = open('%s/%s' % (outdir, 'commonness.err'), 'w')
            for e in err:
                err_out.write('%s\n' % e)
            err_out.close()

    return res, err

def main(p_outlink, p_redirect, outdir, score=False, dump='',
         procs=1, limitmb=1048, multisegment=False):
    if dump:
        print 'loading dump...'
        s = time.time()
        res = json.loads(open(dump).read().encode('utf-8'))
        print time.time() - s
        print 'done.'
    else:
        res, err = parse(p_outlink, p_redirect, score=score, outdir=outdir)
    index(res, '%s/%s' % (outdir, 'commonness_index'),
          procs=procs, limitmb=limitmb, multisegment=multisegment)

if __name__ == '__main__':
    s = time.time()

    if len(sys.argv) != 4:
        print 'USAGE: get_commonness.py ' \
            '<path to outlink flat> <path to redirect flat> <outdir>'
        sys.exit()

    outlink = sys.argv[1]
    redirect = sys.argv[2]
    outdir = sys.argv[3]
    procs = 30
    limitmb = 1024
    multisegment = True
    dump = ''

    main(outlink, redirect, outdir, score=False, dump=dump,
         procs=procs, limitmb=limitmb, multisegment=multisegment)

    print time.time() - s





# from __future__ import division
# #-*- coding: utf-8 -*-
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')
# import re
# import os
# import sys
# sys.setrecursionlimit(5000)
# import time
# from whoosh.index import create_in
# from whoosh.fields import *
# import json

# def split_nested_outlinks(text, content):
#     if len(text) == 0:
#         return
#     else:
#         if text[0:2] == '[[':
#             content.append(list())
#             for i in content:
#                 i.append(text[0:2])
#             text = text[2:]
#             split_nested_outlinks(text, content)
#         elif text[0:2] == ']]':
#             for i in content:
#                 i.append(text[0:2])
#             links.append(''.join(content[-1]))
#             content.pop(-1)
#             text = text[2:]
#             split_nested_outlinks(text, content)
#         else:
#             for i in content:
#                 i.append(text[0])
#             text = text[1:]
#             split_nested_outlinks(text, content)

# def strip_outlink(link):
#     if link.count('[[') > 2 or link.count(']]') > 2:
#         return '', ''
#     if re.match('\[\[\]\]|\[\[\s+\]\]', link):
#         return '', ''
#     m = re.search('\[\[(.+?)\|(.*)\]\]', link)
#     if m != None: # Renamed link
#         title = m.group(1).strip(' ').lstrip(' ')
#         text = m.group(2).strip(' ').lstrip(' ')
#         if re.match('\s+', text) or text == '':
#             text = title
#     else: # Regular link
#         rl  = re.search('\[\[(.+)\]\]', link).group(1).strip(' ').lstrip(' ')
#         title = rl
#         text = rl
#     return title, text

# def get_redirect_table(path):
#     res = dict()
#     for line in open(path, 'r'):
#         tmp = line.strip().split('\t')
#         res[tmp[1]] = tmp[2]
#     return res

# def index(commonness, index_dir, procs=1, limitmb=1048, multisegment=False):
#     schema = Schema(text=TEXT(stored=True),
#                     titles=STORED)

#     if not os.path.exists(index_dir):
#         os.mkdir(index_dir)
#     ix = create_in(index_dir, schema)
#     writer = ix.writer(procs=procs, limitmb=limitmb, multisegment=multisegment)
#     for text in commonness:
#         try:
#             titles = '\n'.join(['%s\t%s' % (title, commonness[text][title]) \
#                                 for title in commonness[text]])
#             writer.add_document(text=text.decode('utf-8'),
#                                 titles=titles.decode('utf-8'))
#         except:
#             print sys.exc_info()
#     writer.commit()

# def parse(p_outlink, p_redirect, p_title, score=False, outdir=''):
#     res = dict()
#     all_titles = set()
#     err = list()
#     rd_table = get_redirect_table(p_redirect)

#     # Count all titles once
#     for line in open(p_title, 'r'):
#         text = line.strip()
#         if re.match('\s+', text) or text == '':
#             continue
#         res[text] = dict()
#         title = line.strip()
#         try:
#             title = rd_table[title]
#         except:
#             pass
#         res[text][title] = 1
#         res[text]['#TOTAL#'] = 1
#         all_titles.add(title)

#     # Count all outlinks
#     for line in open(p_outlink, 'r'):
#         # Parse nested outlink
#         global links
#         links = list()
#         # if line.count('[') != line.count(']'):
#         #     continue
#         try:
#             split_nested_outlinks(line.strip(), list())
#         except:
#             err.append('NESTED: %s' % line.strip())
#             continue

#         for link in links:
#             # # Empty link
#             # if link == '[[]]':
#             #     continue

#             # Strip link
#             try:
#                 title, text = strip_outlink(link)
#             except:
#                 err.append('LINK: %s' % link)
#             if text == '' or title == '':
#                 continue
#             # Revsie title
#             if not title[0].isupper():
#                 title = title.decode('utf-8')
#                 title = title[0].upper() + title[1:]
#                 title = title.encode('utf-8')
#             try:
#                 title = rd_table[title]
#             except:
#                 pass
#             if title not in all_titles:
#                 err.append('TITLE: %s' % title)
#                 continue

#             if text not in res:
#                 res[text] = dict()
#                 res[text]['#TOTAL#'] = 0
#             if title not in res[text]:
#                 res[text][title] = 0
#             res[text][title] += 1
#             res[text]['#TOTAL#'] += 1

#     # Compute Commonness score
#     if score:
#         for text in res:
#             for title in res[text]:
#                 if title == '#TOTAL#':
#                     continue
#                 res[text][title] = res[text][title] / res[text]['#TOTAL#']

#     if outdir:
#         out = open('%s/%s' % (outdir, 'commonness.json'), 'w')
#         out.write(json.dumps(res))
#         out.close()
#         if err:
#             err_out = open('%s/%s' % (outdir, 'commonness.err'), 'w')
#             for e in err:
#                 err_out.write('%s\n' % e)
#             err_out.close()

#     return res, err

# def main(p_outlink, p_redirect, p_title, outdir, score=False, dump='',
#          procs=1, limitmb=1048, multisegment=False):
#     if dump:
#         print 'loading dump...'
#         s = time.time()
#         res = json.loads(open(dump).read().encode('utf-8'))
#         print time.time() - s
#         print 'done.'
#     else:
#         res, err = parse(p_outlink, p_redirect, p_title,
#                          score=score, outdir=outdir)
#     index(res, '%s/%s' % (outdir, 'commonness_index'),
#           procs=procs, limitmb=limitmb, multisegment=multisegment)

# if __name__ == '__main__':
#     s = time.time()

#     # outlink = '/data/m1/panx2/code/wiki-dump-parser/utils/commonness/test/outlink'
#     # redirect = '/data/m1/panx2/code/wiki-dump-parser/utils/commonness/test/redirect'
#     # title = '/data/m1/panx2/code/wiki-dump-parser/utils/commonness/test/title'
#     # outdir = '/data/m1/panx2/code/wiki-dump-parser/utils/commonness/test/'
#     # lang = 'hu'
#     # version = 'huwiki-20160305'
#     lang = 'en'
#     version = 'enwiki-20160305'
#     outlink = '/data/m1/panx2/data/KBs/dump/wiki-dump/%s/output/outlink-%swiki-latest-pages-articles-multistream' % (version, lang)
#     redirect = '/data/m1/panx2/data/KBs/dump/wiki-dump/%s/output/redirect-%swiki-latest-pages-articles-multistream' % (version, lang)
#     title = '/data/m1/panx2/data/KBs/dump/wiki-dump/%s/output/title-%swiki-latest-pages-articles-multistream' % (version, lang)
#     outdir = '/data/m1/panx2/data/KBs/dump/wiki-dump/%s/output/' % (version)
#     procs = 50
#     limitmb = 1024
#     multisegment = True

#     main(outlink, redirect, title, outdir, score=False, dump='',
#          procs=procs, limitmb=limitmb, multisegment=multisegment)

#     print time.time() - s
