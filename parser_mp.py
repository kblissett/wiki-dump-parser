#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import os
import time
import argparse
import multiprocessing
from lxml import etree
from bz2file import BZ2File
from bz2 import decompress
from io import BytesIO
import mwparserfromhell
from src import plaintext
from src import gensimplaintext

'''
 Wikipedia XML dump parser
'''

def fast_iter(pages, select=set(),
              get_outlink=False, get_redirect=False, get_disambiguation=False,
              get_text=False, get_markup=False):
    res = dict()
    err = dict()
    for c in CATEGORY:
        res[c] = list()
        err[c] = list()

    context = etree.iterparse(pages, events=('end',), tag=NAMESPACE+'page')
    for event, elem in context:
        id_ = elem.find(NAMESPACE+'id').text
        if select and id_ not in select:
            continue
        ns = elem.find(NAMESPACE+'ns').text
        if ns != '0': # Main page only
            continue
        title = elem.find(NAMESPACE+'title').text
        text = elem.find(NAMESPACE+'revision').find(NAMESPACE+'text').text
        if text is None:
            continue

        # Outlink: (outlink)
        if get_outlink:
            try:
                wikicode = mwparserfromhell.parse(text)
                # outlinks = wikicode.filter_wikilinks()
                outlinks = [str(i) for i in wikicode.filter_wikilinks()]
                res['outlink'] += outlinks
            except:
                err['outlink'].append((id_, title, sys.exc_info()))

        # Redirect: (id_, title, redirected_title)
        if get_redirect:
            m = elem.find(NAMESPACE+'redirect')
            if m is not None:
                redirected_title = m.attrib['title']
                res['redirect'].append((id_, title, redirected_title))

        # Disambiguation: (id_, title)
        if get_disambiguation:
            if '{{disambiguation}}' in text.lower(): # TO-DO: using re
                res['disambiguation'].append((id_, title))

        # Plain Text: (id_, title, ptext)
        if get_text:
            # ptext = plaintext.get_plaintext(text)
            ptext = gensimplaintext.filter_wiki(text)
            res['text'].append((id_, title, ptext))

        # Wiki Markup: (id_, title, text)
        if get_markup:
            res['markup'].append((id_, title, text))

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context
    return res, err

def get_index(path):
    res = set()
    for line in BZ2File(path):
        m = re.search(('(\d+)\:\d+:.+'), line)
        res.add(int(m.group(1)))
    res = list(sorted(res, key=int))
    res.append(-1)
    return res

result_list = list()
def fast_iter_result(result):
    result_list.append(result)

def main():
    parser = argparse.ArgumentParser(description=\
                                     'Wikipedia XML dump parser')
    parser.add_argument('inpath_xml',
                        help='Path to pages-articles-multistream.xml.bz')
    parser.add_argument('inpath_index',
                        help='Path to pages-articles-multistream-index.txt.bz2')
    parser.add_argument('outdir',
                        help='Output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    parser.add_argument('--outlink', '-o', action='store_true',
                        help='Outlink')
    parser.add_argument('--redirect', '-r', action='store_true',
                        help='Redirect')
    parser.add_argument('--disambiguation', '-d', action='store_true',
                        help='Disambiguation')
    parser.add_argument('--text', '-t', action='store_true',
                        help='Plain text')
    parser.add_argument('--markup', '-m', action='store_true',
                        help='Wiki markup')
    try:
        args = parser.parse_args()
    except IOError, msg:
        parser.error(str(msg))

    inpath_xml = args.inpath_xml
    inpath_index = args.inpath_index
    outdir = args.outdir
    filename = os.path.split(inpath_xml)[1].replace('.xml.bz2', '')
    try:
        os.mkdir(outdir)
    except:
        pass

    global NAMESPACE
    # NAMESPACE = '{http://www.mediawiki.org/xml/export-0.10/}'
    NAMESPACE = ''
    global CATEGORY
    CATEGORY = ['outlink', 'redirect', 'disambiguation', 'text', 'markup']

    bz2f = open(inpath_xml, 'rb')
    bz2f_index = get_index(inpath_index)
    bz2f.seek(bz2f_index[0])
    pool = multiprocessing.Pool(processes=int(args.nworker))
    for i in bz2f_index:
        if i == bz2f_index[0]:
            continue
        pages = '<pages>\n%s</pages>\n' % decompress(bz2f.read(i))
        if i == -1:
            pages = pages.replace('</mediawiki>', '')
        else:
            bz2f.seek(i)

        pool.apply_async(fast_iter, args=(BytesIO(pages), set(),
                                          args.outlink, args.redirect,
                                          args.disambiguation, args.text,
                                          args.markup, ),
                         callback=fast_iter_result)
    pool.close()
    pool.join()

    errors = dict()
    for c in CATEGORY:
        errors[c] = list()

    # Outlink
    if args.outlink:
        out = open('%s/%s-%s' % (outdir, 'outlink', filename), 'w')
        for r in result_list:
            res = r[0]
            for i in res['outlink']:
                out.write(i + '\n')
            err = r[1]
            errors['outlink'] += err['outlink']
        out.close()

    # Redirect
    if args.redirect:
        out = open('%s/%s-%s' % (outdir, 'redirect', filename), 'w')
        for r in result_list:
            res = r[0]
            for i in res['redirect']:
                out.write('\t'.join(i) + '\n')
            err = r[1]
            errors['redirect'] += err['redirect']
        out.close()

    # Disambiguation
    if args.disambiguation:
        out = open('%s/%s-%s' % (outdir, 'disambiguation', filename), 'w')
        for r in result_list:
            res = r[0]
            for i in res['disambiguation']:
                out.write('\t'.join(i) + '\n')
            err = r[1]
            errors['disambiguation'] += err['disambiguation']
        out.close()

    # Plain Text
    if args.text:
        try:
            os.mkdir('%s/text' % outdir)
        except:
            pass
        for r in result_list:
            res = r[0]
            for i in res['text']:
                id_, title, ptext = i
                if os.path.isfile('%s/text/%s' % (outdir, id_)):
                    errors['text'].append((id_, title, 'Duplicated ID'))
                    continue
                out = open('%s/text/%s' % (outdir, id_), 'w')
                out.write('%s\t%s\n%s' % (id_, title, ptext))
                out.close()

    # Wiki Markup
    if args.markup:
        try:
            os.mkdir('%s/markup' % outdir)
        except:
            pass
        for r in result_list:
            res = r[0]
            for i in res['markup']:
                id_, title, markup = i
                if os.path.isfile('%s/markup/%s' % (outdir, id_)):
                    errors['markup'].append((id_, title, 'Duplicated ID'))
                    continue
                out = open('%s/markup/%s' % (outdir, id_), 'w')
                out.write('%s\t%s\n%s' % (id_, title, markup))
                out.close()
    # Errors
    for c in CATEGORY:
        if errors[c]:
            out_err = open('%s/%s-%s.err' % (outdir, c, filename), 'w')
            for i in errors[c]:
                out_err.write(str(i) + '\n')
            out_err.close()

if __name__ == '__main__':
    s = time.time()
    main()
    print time.time() - s