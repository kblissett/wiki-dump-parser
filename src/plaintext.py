import re

def unhtml(html):
    """
    Remove HTML from the text.
    """
    html = re.sub(r'(?i)&nbsp;', ' ', html)
    html = re.sub(r'(?i)<br[ \\]*?>', '\n', html)
    html = re.sub(r'(?m)<!--.*?--\s*>', '', html)
    html = re.sub(r'(?i)<ref[^>]*>[^>]*<\/ ?ref>', '', html)
    html = re.sub(r'(?m)<.*?>', '', html)
    html = re.sub(r'(?i)&amp;', '&', html)

    return html

def unwiki(wiki):
    """
    Remove wiki markup from the text.
    """
    wiki = re.sub(r'(?i)\{\{IPA(\-[^\|\{\}]+)*?\|([^\|\{\}]+)(\|[^\{\}]+)*?\}\}', lambda m: m.group(2), wiki)
    wiki = re.sub(r'(?i)\{\{Lang(\-[^\|\{\}]+)*?\|([^\|\{\}]+)(\|[^\{\}]+)*?\}\}', lambda m: m.group(2), wiki)
    wiki = re.sub(r'\{\{[^\{\}]+\}\}', '', wiki)
    wiki = re.sub(r'(?m)\{\{[^\{\}]+\}\}', '', wiki)
    wiki = re.sub(r'(?m)\{\|[^\{\}]*?\|\}', '', wiki)
    wiki = re.sub(r'(?i)\[\[Category:[^\[\]]*?\]\]', '', wiki)
    wiki = re.sub(r'(?i)\[\[Image:[^\[\]]*?\]\]', '', wiki)
    wiki = re.sub(r'(?i)\[\[File:[^\[\]]*?\]\]', '', wiki)
    wiki = re.sub(r'\[\[[^\[\]]*?\|([^\[\]]*?)\]\]', lambda m: m.group(1), wiki)
    wiki = re.sub(r'\[\[([^\[\]]+?)\]\]', lambda m: m.group(1), wiki)
    wiki = re.sub(r'\[\[([^\[\]]+?)\]\]', '', wiki)
    wiki = re.sub(r'(?i)File:[^\[\]]*?', '', wiki)
    wiki = re.sub(r'\[[^\[\]]*? ([^\[\]]*?)\]', lambda m: m.group(1), wiki)
    wiki = re.sub(r"''+", '', wiki)
    wiki = re.sub(r'(?m)^\*$', '', wiki)

    return wiki

def punctuate(text):
    """
    Convert every text part into well-formed one-space
    separate paragraph.
    """
    text = re.sub(r'\r\n|\n|\r', '\n', text)
    text = re.sub(r'\n\n+', '\n\n', text)

    parts = text.split('\n\n')
    partsParsed = []

    for part in parts:
        part = part.strip()

        if len(part) == 0:
            continue

        partsParsed.append(part)

    return '\n\n'.join(partsParsed)

def get_plaintext(text):
    # get plain text
    text = unhtml(text)
    text = unwiki(text)
    text = punctuate(text)

    return text
