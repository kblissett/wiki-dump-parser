#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import os
from lxml import etree
import argparse
import mwparserfromhell
from src import plaintext
from src import gensimplaintext

'''
 Wikipedia XML dump parser
'''

def fast_iter(context, res, err, select=set(),
              get_outlink=False, get_redirect=False, get_disambiguation=False,
              get_text=False, get_markup=False):
    for event, elem in context:
        id_ = elem.find(NAMESPACE+'id').text
        if select and id_ not in select:
            continue
        title = elem.find(NAMESPACE+'title').text
        text = elem.find(NAMESPACE+'revision').find(NAMESPACE+'text').text
        if text is None:
            continue

        # Outlink: (outlink)
        if get_outlink:
            try: # TODO: replace moudle mwparserfromhell
                wikicode = mwparserfromhell.parse(text)
                outlinks = wikicode.filter_wikilinks()
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
            if '{{disambiguation}}' in text.lower():
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

def main():
    parser = argparse.ArgumentParser(description=\
                                     "Wikipedia XML dump parser")
    parser.add_argument("inpath",
                        help='Path to wiki-pages-articles-multistream.xml')
    parser.add_argument("outdir",
                        help='Output dir')
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

    inpath = args.inpath
    outdir = args.outdir
    path = os.path.split(inpath)[0]
    filename = os.path.split(inpath)[1].replace('.xml', '')
    try:
        os.mkdir(outdir)
    except:
        pass

    global NAMESPACE
    NAMESPACE = '{http://www.mediawiki.org/xml/export-0.10/}'

    cat = ['outlink', 'redirect', 'disambiguation', 'text', 'markup']
    res = dict()
    err = dict()
    for c in cat:
        res[c] = list()
        err[c] = list()
    context = etree.iterparse(inpath, events=('end',), tag=NAMESPACE+'page')
    fast_iter(context, res, err, select=set(),
              get_outlink=args.outlink,
              get_redirect=args.redirect,
              get_disambiguation=args.disambiguation,
              get_text=args.text,
              get_markup=args.markup)

    # Outlink
    if args.outlink:
        out = open('%s/%s-%s' % (outdir, 'outlink', filename), 'w')
        for i in res['outlink']:
            out.write(str(i) + '\n')
        out.close()

    # Redirect
    if args.redirect:
        out = open('%s/%s-%s' % (outdir, 'redirect', filename), 'w')
        for i in res['redirect']:
            out.write('\t'.join(i) + '\n')
        out.close()

    # Disambiguation
    if args.disambiguation:
        out = open('%s/%s-%s' % (outdir, 'disambiguation', filename), 'w')
        for i in res['disambiguation']:
            out.write('\t'.join(i) + '\n')
        out.close()

    # Plain Text
    if args.text:
        try:
            os.mkdir('%s/text' % outdir)
        except:
            pass
        for i in res['text']:
            id_, title, ptext = i
            if os.path.isfile('%s/text/%s' % (outdir, id_)):
                err['text'].append((id_, title, 'Duplicated ID'))
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
        for i in res['markup']:
            id_, title, markup = i
            if os.path.isfile('%s/markup/%s' % (outdir, id_)):
                err['markup'].append((id_, title, 'Duplicated ID'))
                continue
            out = open('%s/markup/%s' % (outdir, id_), 'w')
            out.write('%s\t%s\n%s' % (id_, title, markup))
            out.close()

    for c in cat:
        if err[c]:
            out_err = open('%s/%s-%s.err' % (outdir, c, filename), 'w')
            for i in err[c]:
                out_err.write(str(i) + '\n')
            out_err.close()

if __name__ == '__main__':
    main()
