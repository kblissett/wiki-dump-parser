#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import os
from lxml import etree
import mwparserfromhell
from src import plaintext
from src import gensimplaintext

'''
 Wikipedia XML dump parser
'''

def fast_iter_outlink(context, outdir, name):
    out = open('%s/outlink-%s' % (outdir, name), 'w')
    for event, elem in context:
        try:
            wikicode = mwparserfromhell.parse(elem.text)
            outlinks = wikicode.filter_wikilinks()
            for i in outlinks:
                out.write('%s\n' % i)
        except:
            err = open('%s/outlink.error' % outdir, 'aw')
            err.write('OUTLINK: %\s\t%s\n' % (name, sys.exc_info()[0]))
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

def find_redirect(elem, out):
    title = elem.find(namespace+'title').text
    m = elem.find(namespace+'redirect')
    if m != None:
        redirect = m.attrib['title']
        out.write('%s\t%s\n' % (title, redirect))

def fast_iter_redirect(context, outdir, name):
    out = open('%s/redirect-%s' % (outdir, name), 'w')
    for event, elem in context:
        try:
            find_redirect(elem, out)
        except:
            err = open('%s/redirect.error' % outdir, 'aw')
            title = elem.find(namespace+'title').text
            err.write('REDIRECT: %s\t%s\t%s\n' % \
                      (name, title, sys.exc_info()[0]))
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

def fast_iter_disambiguation(context, outdir, name):
    out = open('%s/disambiguation-%s' % (outdir, name), 'w')
    for event, elem in context:
        title = elem.find(namespace+'title').text
        text = elem.find(namespace+'revision').find(namespace+'text').text
        try:
            if text != None and '{{disambiguation}}' in text.lower():
                out.write('%s\n' % title)
        except:
            err = open('%s/disambiguation.error' % outdir, 'aw')
            err.write('DISAMBIGUATION: %s\t%s\t%s\n' % \
                      (name, title, sys.exc_info()[0]))
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

def fast_iter_text(context, outdir, name, select=set()):
    for event, elem in context:
        id_ = elem.find(namespace+'id').text
        if select != set() and id_ not in select:
            continue
        title = elem.find(namespace+'title').text
        text = elem.find(namespace+'revision').find(namespace+'text').text
        try:
            if text == None:
                ptext = ''
            else:
                # ptext = plaintext.get_plaintext(text)
                ptext = gensimplaintext.filter_wiki(text)
            if os.path.isfile('%s/%s' % (outdir, id_)):
                raise Exception('Duplicated ID: %s' % id_)
            out = open('%s/%s' % (outdir, id_), 'w')
            out.write('%s\t%s\n%s' % (id_, title, ptext))
        except:
            err = open('%s/text.error' % outdir, 'aw')
            err.write('TEXT: %s\t%s\t%s\n' % \
                      (name, title, sys.exc_info()[0]))
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

def fast_iter_title(context, outdir, name):
    out = open('%s/title-%s' % (outdir, name), 'w')
    for event, elem in context:
        id_ = elem.find(namespace+'id').text
        title = elem.find(namespace+'title').text
        try:
            out.write('%s:%s\n' % (id_, title))
        except:
            err = open('%s/title.error' % outdir, 'aw')
            err.write('TITLE: %s\t%s\t%s\n' % \
                      (name, title, sys.exc_info()[0]))
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

# def fast_iter_pair(context):
#     for event, elem in context:
#         if elem.text != None:
#             try: # TODO: find more patterns
#                 # tmp = re.findall('\{\{\S{2}\|\S\=.*?\}\}', elem.text)
#                 # for i in tmp:
#                 #     out.write('%s\n' % i)
#                 ptext = gensimplaintext.filter_wiki(elem.text)
#                 tmp = re.findall('\(Hausa\: .+\)', ptext)
#                 for i in tmp:
#                     out.write('%s\n' % i)
#             except:
#                 err.write('%s\n%s\n' % (name, elem.text))

#         elem.clear()
#         while elem.getprevious() is not None:
#             del elem.getparent()[0]
#     del context

def fast_iter_markup(context, outdir, name, select=set()):
    for event, elem in context:
        id_ = elem.find(namespace+'id').text
        if select != set() and id_ not in select:
            continue
        title = elem.find(namespace+'title').text
        markup = elem.find(namespace+'revision').find(namespace+'text').text
        '''
        title (str): wiki page title
        markup (str): wiki markup of this page
        '''
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context

def main():
    if len(sys.argv) != 3:
        print 'USAGE: wiki-dump_parser.py' \
            ' <langwiki-latest-pages-articles-multistream.xml>' \
            ' <output directory>'
        sys.exit()
    infile = sys.argv[1]
    outdir = sys.argv[2]

    path = os.path.split(infile)[0]
    name = os.path.split(infile)[1].replace('.xml', '')
    try:
        os.mkdir(outdir)
    except:
        pass

    global namespace
    namespace = '{http://www.mediawiki.org/xml/export-0.10/}'

    # ### Outlink
    # context = etree.iterparse(infile, events=('end',), tag=namespace+'text')
    # fast_iter_outlink(context, outdir, name)

    # ### Redirect
    # context = etree.iterparse(infile, events=('end',), tag=namespace+'page')
    # fast_iter_redirect(context, outdir, name)

    # ### Disambiguation
    # context = etree.iterparse(infile, events=('end',), tag=namespace+'page')
    # fast_iter_disambiguation(context, outdir, name)

    # ### PLAIN TEXT
    # try:
    #     os.mkdir('%s/text' % outdir)
    # except:
    #     pass
    # context = etree.iterparse(infile, events=('end',), tag=namespace+'page')
    # fast_iter_text(context, '%s/text' % outdir, name, select=set())

    # ### TITLE
    # context = etree.iterparse(infile, events=('end',), tag=namespace+'page')
    # fast_iter_title(context, outdir, name)

if __name__ == '__main__':
    main()
