#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import os
import io
import time
import argparse
import multiprocessing
from lxml import etree
import bz2file
import mwparserfromhell
from src import markup2text
from src import markup2textwl

'''
 Wikipedia XML dump parser
'''

PWD=os.path.dirname(os.path.abspath(__file__))
def fast_iter(beg, end, xmlpath,
              get_outlink=False, get_redirect=False, get_disambiguation=False,
              get_title=False, get_text=False, get_markup=False):
    res = dict()
    err = dict()
    for c in CATEGORY:
        res[c] = list()
        err[c] = list()
    try:
        # print '%s starts' % os.getpid()
        bz2f = io.open(xmlpath, 'rb')
        bz2f.seek(beg)
        if end == -1:
            blocks = bz2f.read(-1)
        else:
            blocks = bz2f.read(end - beg)

        xml = bz2file.BZ2File(io.BytesIO(blocks))
        pages = '<pages>\n%s</pages>\n' % xml.read()
        if end == -1:
            pages = pages.replace('</mediawiki>', '')

        context = etree.iterparse(io.BytesIO(pages),
                                  events=('end',), tag=NAMESPACE+'page')
        for event, elem in context:
            id_ = elem.find(NAMESPACE+'id').text
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
                    # Markup with outlinks only
                    markup_wl = markup2textwl.filter_wiki(text,
                                                          image=RE_IMAGE,
                                                          file=RE_FILE,
                                                          category=RE_CAT)
                    wikicode = mwparserfromhell.parse(markup_wl)
                    outlinks = [title] + \
                               [re.sub('\n|\t', '', str(i)) \
                                for i in wikicode.filter_wikilinks()]
                    res['outlink'].append(outlinks)
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
                if '{{disambiguation}}' in text.lower(): # TO-DO: use re
                    res['disambiguation'].append((id_, title))

            # Title: (title)
            if get_title:
                res['title'].append(title)

            # Plain Text: (id_, title, ptext)
            if get_text:
                ptext = markup2text.filter_wiki(text,
                                                image=RE_IMAGE,
                                                file=RE_FILE,
                                                category=RE_CAT)
                # ptext = markup2textwl.filter_wiki(text,
                #                                   image=RE_IMAGE,
                #                                   file=RE_FILE,
                #                                   category=RE_CAT)
                ptext = '\t'.join(filter(None,
                                         ptext.replace('\t', ' ').split('\n')))
                res['text'].append((id_, title, ptext))

            # Wiki Markup: (id_, title, markup)
            if get_markup:
                markup = '\t'.join(filter(None,
                                          text.replace('\t',' ').split('\n')))
                res['markup'].append((id_, title, markup))

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del context
        # print '%s done.' % os.getpid()
        return res, err

    except:
        print sys.exc_info()
        return res, err

def get_index(path):
    res = set()
    for line in bz2file.BZ2File(path):
        m = re.search(('(\d+)\:\d+:.+'), line)
        res.add(int(m.group(1)))
    res = list(sorted(res, key=int))
    res.append(-1)
    return res

result_list = list()
def fast_iter_result(result):
    result_list.append(result)

def load_re_patterns():
    res = dict()
    for line in open('%s/re_patterns' % PWD):
        tmp = line.strip().split(' ')
        res[tmp[0]] = (tmp[1], tmp[2], tmp[3])
    return res

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
    parser.add_argument('--title', '-t', action='store_true',
                        help='Title')
    parser.add_argument('--text', '-p', action='store_true',
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
    lang = re.search('(\w+)wiki\-', filename).group(1)

    try:
        os.mkdir(outdir)
    except:
        pass

    global NAMESPACE
    # NAMESPACE = '{http://www.mediawiki.org/xml/export-0.10/}'
    NAMESPACE = ''
    re_patterns = load_re_patterns()
    global RE_IMAGE
    global RE_FILE
    global RE_CAT
    if lang in re_patterns:
        RE_IMAGE = re_patterns[lang][0]
        RE_FILE = re_patterns[lang][1]
        RE_CAT = re_patterns[lang][2]
    else:
        RE_IMAGE = re_patterns['en'][0]
        RE_FILE = re_patterns['en'][1]
        RE_CAT = re_patterns['en'][2]
    print 'Loaded re patterns:\nIMAGE: %s\nFILE: %s\nCATEGORY: %s' % \
        (RE_IMAGE, RE_FILE, RE_CAT)
    global CATEGORY
    CATEGORY = ['outlink', 'redirect', 'disambiguation',
                'title', 'text', 'markup']

    bz2f_index = get_index(inpath_index)
    pool = multiprocessing.Pool(processes=int(args.nworker))
    print 'Number of workers: %s' % args.nworker
    print 'Processing...'
    for i in zip(bz2f_index, bz2f_index[1:]):
        pool.apply_async(fast_iter, args=(i[0], i[1], inpath_xml,
                                          args.outlink, args.redirect,
                                          args.disambiguation, args.title,
                                          args.text, args.markup, ),
                         callback=fast_iter_result)
    pool.close()
    pool.join()

    errors = dict()
    for c in CATEGORY:
        errors[c] = list()

    # Outlink
    if args.outlink:
        out = open('%s/%s-%s' % (outdir, 'outlink', lang), 'w')
        for r in result_list:
            res = r[0]
            for i in res['outlink']:
                out.write('\t'.join(i) + '\n')
            err = r[1]
            errors['outlink'] += err['outlink']
        out.close()

    # Redirect
    if args.redirect:
        out = open('%s/%s-%s' % (outdir, 'redirect', lang), 'w')
        for r in result_list:
            res = r[0]
            for i in res['redirect']:
                out.write('\t'.join(i) + '\n')
            err = r[1]
            errors['redirect'] += err['redirect']
        out.close()

    # Disambiguation
    if args.disambiguation:
        out = open('%s/%s-%s' % (outdir, 'disambiguation', lang), 'w')
        for r in result_list:
            res = r[0]
            for i in res['disambiguation']:
                out.write('\t'.join(i) + '\n')
            err = r[1]
            errors['disambiguation'] += err['disambiguation']
        out.close()

    # Title
    if args.title:
        out = open('%s/%s-%s' % (outdir, 'title', lang), 'w')
        for r in result_list:
            res = r[0]
            for i in res['title']:
                out.write(i + '\n')
            err = r[1]
            errors['title'] += err['title']
        out.close()

    # Plain Text
    if args.text:
        out = open('%s/%s-%s' % (outdir, 'ptext', lang), 'w')
        for r in result_list:
            res = r[0]
            for id_, title, ptext in res['text']:
                out.write('%s\t%s\t%s\n' % (id_, title, ptext))
        out.close()

    # Wiki Markup
    if args.markup:
        out = open('%s/%s-%s' % (outdir, 'markup', lang), 'w')
        for r in result_list:
            res = r[0]
            for id_, title, markup in res['markup']:
                out.write('%s\t%s\t%s\n' % (id_, title, markup))
        out.close()

    # Errors
    for c in CATEGORY:
        if errors[c]:
            out_err = open('%s/%s-%s.err' % (outdir, c, lang), 'w')
            for i in errors[c]:
                out_err.write(str(i) + '\n')
            out_err.close()

if __name__ == '__main__':
    s = time.time()
    main()
    print 'Total Time: %s' % (time.time() - s)
