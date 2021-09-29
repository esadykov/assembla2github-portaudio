""" utility for migrating github -> assembla """
import argparse
from datetime import datetime, timezone
import logging
import json
import string
import sys
import git
import pathlib
from tabulate import tabulate
from pprint import pprint
import requests
import time
import github
import re
import itertools
import colorama
import functools

# Ensure colored output on win32 platforms
colorama.init()

TOOLVERSION=5
TOOLDATE="2020-09-24"

# Map Assembla field values to GitHub lables. The value 'None' indicates that
# the field will be omitted.
ASSEMBLA_TO_GITHUB_LABELS = {
    'status': {
        # Examples:
        'New': None,
        'WorksForMe': 'invalid',
    },
    'priority': {
        'Highest (1)': 'P1',
        'High (2)': 'P2',
        'Normal (3)': 'P3',
        'Low (4)': 'P4',
        'Lowest (5)': 'P5',
    },
    'tags': {
    },
    'component': {
    },
    'keywords': {
    }
}

# New GitHub labels to create. The value is the RGB hex color for that label.
# For reference, GitHub comes with the following default labels:
#   'bug': 'd73a4a',
#   'documentation': '0075ca',
#   'duplicate': 'cfd3d7',
#   'enhancement': 'a2eeef',
#   'good first issue': '7057ff',
#   'help wanted': '008672',
#   'invalid': 'e4e669',
#   'question': 'd876e3',
#   'wontfix': 'ffffff',
NEW_GITHUB_LABELS = {
}

# Mapping from Assemblas numerical priority format to their string format
ASSEMBLA_PRIORITY_MAPPING = {
    1: "Highest (1)",
    2: "High (2)",
    3: "Normal (3)",
    4: "Low (4)",
    5: "Lowest (5)",
}

# User mapping from assembla to github
#   - login: Assembla user name. Used to match tickets "assigned to" fields
#   - name: Presented name. Used for wiki git commits and tickets (if no github id exists)
#   - email: Used for wiki git commits
#   - github: GitHub user name. Used for tickets as @mentions
ASSEMBLA_USERID = {
    # Please fill in with githubid:
    #   'userid': {'github': '<githubusername>'}
    #
    # Each line consists of
    # 'userid              ': { },  # login                 name

    # Dummy user (for ticket import)
    'dummy': {'id': 'dummy', 'login': 'dummy', 'name': 'GitHub importer', 'email': None, 'github': None},
}

# Settings for Wiki conversions
WIKI_MENU_HEADING = "# Title"
WIKI_FIXUP_AUTHOR_NAME = "Wiki converter"
WIKI_FIXUP_AUTHOR_EMAIL = "none@localhost"
WIKI_FIXUP_MESSAGE = "Updated Wiki to GitHub formatting"
WIKI_UNKNOWN_EMAIL = "none@localhost"

# URLs to replace when converting Wiki
URL_RE_REPLACE = [
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/tickets$', r'{GITHUB_URL}/issues'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/tickets\.$', r'{GITHUB_URL}/issues.'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/tickets/new$', r'{GITHUB_URL}/issues/new'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/wiki$', r'{GITHUB_URL}/wiki'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/milestones$', r'{GITHUB_URL}/milestones'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/git/source$', r'{GITHUB_URL}/'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/git/source/([^\?]*)(\?.*)?$', r'{GITHUB_URL}/tree/\2'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/git/commits/list$', r'{GITHUB_URL}/commits'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/git/commits/(\w+)$', r'\2'),
    (r'^https?://git\.assembla\.com/{ASSEMBLA_SPACE}\.git$', r'{GITHUB_URL}.git'),
]
URL_RE_REPLACE_WIKI = [
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/tickets/(\d+)$', r'[#\2](../issues/\2)'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/wiki/(.*)$', r'[[\2]]'),
]
URL_RE_REPLACE_TICKETS = [
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/tickets/(\d+)$', r'#\2'),
    (r'^https?://(www|app)\.assembla\.com/spaces/{ASSEMBLA_SPACE}/wiki/(.*)$', r'[\2](../wiki/\2)'),
]
_URL_RE = []
_URL_RE_WIKI = []
_URL_RE_TICKETS = []

# Polling exponential delay
POLL_INITIAL = 0.1
POLL_FACTOR = 1.628347746
POLL_MAX_DELAY = 8
POLL_MAX_FAILS = 8

class UnsetMeta(type):
    def __repr__(self):
        return "<Unset>"


class Unset(metaclass=UnsetMeta):
    """ Unset class """


# Inheriting dict isn't recommended, but this is a small mixin so it is probably ok for this use
class DictPlus(dict):
    """ dict mixin class with extra convenience methods """

    def find(self, table, id, default=Unset):
        if default is Unset:
            return self['_index'][table][id]
        return self['_index'][table].get(id, default)


def findfirst(fn, collection, default=None):
    """
    Return the first match of fn in collection. If not match found 'default' is
    returned.
    """
    return next(iter(filter(fn, collection)), default)


def dig(obj, *keys):
    """
    Return value index recursively, e.g. dig(a, b, c) will return a[b][c]. It
    is safe against None-valued mid-values, returning None.
    """
    o = obj
    for k in keys:
        o = o.get(k)
        if not o:
            return o
    return o


def nameorid(user):
    """ Return the name or the id of the user """
    if not user:
        return user
    return user.get('name', user.get('id'))


def githubuser(user):
    # First try to return '@<githubusername>'
    if not user:
        return user
    ghuser = None
    if user:
        ghuser = user.get('github')
    if ghuser:
        return f"@{ghuser}"
    # If there is no valid username, return the given name
    login = user['login']
    if login == 'name@domain':
        return user['name']
    # Return the assembla user name
    return login


def githubassignee(user, number):
    ghuser = None
    if user:
        ghuser = user.get('github')
    if ghuser:
        return [ghuser]
    if user:
        logging.warning(f"Cannot assign issue #{number} to {user.get('login')}/{user.get('name')} ({user.get('id')}), missing github identity")
    return None


def githubcreatedheader(user, date=None):
    dt = f' at {date}' if date else ''
    name = githubuser(user)
    if name.startswith('@'):
        return f"*Issue created by {name}{dt}*"
    return f"*Issue created by {name} on Assembla{dt}*"


def githubcommentedheader(user, date=None):
    dt = f' at {date}' if date else ''
    name = githubuser(user)
    if name.startswith('@'):
        return f"*Comment by {name}{dt}*"
    return f"*Comment by {name} on Assembla{dt}*"


def githubeditedheader(user, date=None, edit='edited'):
    dt = f' at {date}' if date else ''
    name = githubuser(user)
    if name.startswith('@'):
        return f"*Issue {edit} by {name}{dt}*"
    return f"*Issue {edit} by {name} on Assembla{dt}*"


def githubstate(state):
    return 'open' if state else 'closed'


def githubtime(date):
    if not date:
        return date
    t = date.isoformat()
    if t.endswith('+00:00'):
        t = t.replace('+00:00','')
    return t + 'Z'


def transpose(data, keys=None):
    """
    Transpose the given dict.
    :param data: Dict indexed by id containing rows as values, where each row is
                 a dictionary with columns as keys
    :param keys: List of keys to include in transpose
    :returns: Transposed dictionary. Dict indexed by keys/columns containing
              arrays of rows.
    """
    if not data:
        return {}
    if not keys:
        keys = list(data[0])
    rawlist = [[v.get(k) for k in keys] for v in data]
    transposed = list(map(list, zip(*rawlist)))
    return {k: transposed[i] for i, k in enumerate(keys)}


def flatten(iter):
    """ Flatten the iterator iter into a one-dimensional list """
    out = []
    for x in iter:
        if not x:
            continue
        if isinstance(x, (list, tuple, set)):
            out += flatten(x)
        else:
            out.append(x)
    return out


def printtable(data, keys=None, exclude=None, include=None, filter=None, slice=None):
    """
    Print the data formatted in tables.
    :param data: Dict or list containing rows.
    :param keys: List of keys to include in transpose
    :param exclude: List of keys to omit from output
    :param include: List of keys to include in output
    :param filter: Callback function fn(row) to filter rows to print
    :param slice: Pass a slice object to limit the number of lines
    """
    if isinstance(data, dict):
        data = list(data.values())
    if filter:
        data = [v for v in data if filter(v)]
    if slice:
        data = data[slice]
    data = transpose(data, keys)
    if not exclude:
        exclude = []
    if not include:
        include = []
    for k in list(data.keys()):
        if k in include:
            continue
        if k in exclude or k.startswith('_'):
            del data[k]
    print(tabulate(data, headers="keys"))


def mapjsonlinetoassembblaobject(jsonstring, fieldlist, linenum, linetype):
    """
    converts json string -> dict
    :param jsonstring: string array "['a', 123, ...]"
    :param fieldlist: expected ordered list of fields expected in json array
    :param linenum: current line num
    :param linetype: for the error message report if needed. tells us the type of line we are trying to read
    :returns: a dict with the values from the jsonstring and the keys from the fieldlist
    """
    logging.debug('attempting to parse line #{0} as a {1}'.format(linenum, linetype))
    arr = json.loads(jsonstring)
    if len(arr) != len(fieldlist):
        raise AssertionError('Assertion fail: {3} line [{0}] actual fields [{1}] != expected fields [{2}]'.format(linenum, len(arr), len(fieldlist), linetype))
    return {field: value for field, value in zip(fieldlist, arr)}


def filereadertoassemblaobjectgenerator(filereader, fieldmap):
    """
    File reader to assembla object generator
    :param filereader: File object which is read line by line
    :returns: Generator which yields tuple (linenum, line, linetype, assemblaobject)
    """

    # for each line determine the assembla object type, read all attributes to dict using the mappings
    # assign a key for each object which is used to link github <-> assembla objects to support updates
    for linenum, line in enumerate(filereader.readlines()):

        # Remove all non printable characters from the line
        _line = line.rstrip()
        # line = ''.join(x for x in _line if x in string.printable)

        if line != _line:
            logging.debug(f"line #{linenum}: Unprintable chars in '{line}'")
        logging.debug(f"line #{linenum}: {line}")

        # Parse the field definition if present
        fields = line.split(':fields, ')
        if len(fields) > 2:
            logging.error(f"line #{linenum}: Unexpected field count in '{line}'")
            continue
        if len(fields) > 1:
            key = fields[0]
            fieldmap[key] = json.loads(fields[1])
            continue

        # Parse the table entry
        heading = line.split(', [')
        if len(heading) < 2:
            logging.error(f"line #{linenum}: Unexpected syntax in '{line}'")
            continue
        table = heading[0]
        if table not in fieldmap:
            logging.error("line #{linenum}: Table '{table}' not defined before '{line}'")
            continue
        currentline = line.replace(table + ', ', '').strip()
        row = mapjsonlinetoassembblaobject(currentline, fieldmap[table], linenum, table)

        yield (linenum, line, table, row)


def indexassembladata(data, keymap):
    """
    Convert each table in data dict from list of rows to dict indexed by key
    specified in keymap.
    :param data: Dict indexed by tablename containing list of rows
    :param keymap: A dict indexed by tablename containing the key field.
    :returns: Dict indexed by tablename containing a dict indexed by keys.
    """

    # keymap[None] contains the default key field name
    default = keymap.get(None)

    index = {}
    for table, objects in data.items():

        # Get the key field name. If None, keep skip the table
        key = keymap.get(table, default)
        if key is None or table.startswith('_'):
            continue

        ids = [k[key] for k in objects]
        # if not ids:  # Skip empty tables
        #    continue
        if len(ids) != len(set(ids)):
            logging.warning(f"Non unique id in table '{table}', {len(set(ids))} unique of {len(ids)} rows")

        # Append the table data into a dict
        index[table] = {k[key]: k for k in objects}

    return index


def wikiparser(data):
    """
    Parse the wiki tables
    :param data: assembla dataset
    :returns: A list of sorted wiki pages in presentation order
    """

    # wiki_pages
    # ==========
    #   change_comment, contents, created_at, id, page_name, parent_id, position, space_id, status,
    #   updated_at, user_id, version, wiki_format
    wikitree = {}
    for v in data['wiki_pages']:

        # Add the reference to the parent and children
        # v['_parent'] = data.find('wiki_pages', v['parent_id'], None)
        v.setdefault('_children', [])

        # Add the reference to the user
        v['_user'] = data.find('_users', v['user_id'])

        # Convert dates
        v['_created_at'] = datetime.fromisoformat(v['created_at'])
        v['_updated_at'] = datetime.fromisoformat(v['updated_at'])

        # Append element to the wiki directory list
        parent = v['parent_id']
        wikitree.setdefault(parent, [])
        wikitree[parent].append(v)

        if parent:
            # Link parent to child list and increse the level on this row
            parentobj = data.find('wiki_pages', parent)
            parentobj['_children'] = wikitree[parent]
            v['_level'] = parentobj.get('_level', 0) + 1
        else:
            v['_level'] = 0

    # DEBUG
    # printtable(data['wiki_pages'], include=('_level', ))

    # wiki_page_blobs
    # ===============
    #    blob_id, version_id

    # wiki_page_versions
    # ==================
    #   change_comment, contents, created_at, id, updated_at, user_id, version, wiki_page_id
    for v in data['wiki_page_versions']:

        # Add reference to the blob
        # v['_blob_id'] = data.find('wiki_page_blobs', v['id']).get('blob_id')

        # Add reference to the wiki page object
        v['_wiki_page'] = data.find('wiki_pages', v['wiki_page_id'])

        # Add the user
        v['_user'] = data.find('_users', v['user_id'])

        # Convert dates
        v['_created_at'] = datetime.fromisoformat(v['created_at'])
        v['_updated_at'] = datetime.fromisoformat(v['updated_at'])

    # DEBUG
    # printtable(data['wiki_page_versions'], include=('_blob_id', ))

    def _wikitraverse(tree):
        """ Generator to produce all wiki pages in order from top to bottom """
        for v in sorted(tree, key=lambda v: v['position']):
            yield v
            if '_children' in v:
                yield from _wikitraverse(v['_children'])

    return list(_wikitraverse(wikitree[None]))


def mergewikidata(wikidata, wiki_page_versions):
    """
    Merge incoming wikidata with the main data dict
    :param wikidata: imported wiki page dataset from file fetched with wikidump
    :param wiki_page_versions: dict of all wiki page version which the data will be
                               inserted into.
    """

    # Data is arranged as [PAGE1,PAGE2,...] where PAGE is [VER1,VER2,...]
    # which itertools.chain() will flatten
    count = 0
    for v in itertools.chain(*wikidata):
        count += 1
        # Get the corresponding wiki page data from the dump
        w = wiki_page_versions.get(v['id'])
        if not w:
            logging.warning(f"Skipping wiki page '{v['id']}'. Not found in main dump file")
            continue

        # Ensure the data contains the same keys
        vkeys = set(v.keys())
        wkeys = set(w.keys())

        k = vkeys.difference(wkeys)
        if k:
            logging.warning(f"Wiki page '{v['id']}' contains keys not in main dump file {k}")
        k = wkeys.difference(vkeys)
        if k:
            logging.warning(f"Wiki page '{v['id']}' missing keys {k}")

        for k in v:
            if k in ('contents', ):
                continue
            left, right = v[k], w[k]
            if k in ('created_at', 'updated_at'):
                if left.endswith('Z'):
                    left = left[:-1] + '+00:00'
            if left != right:
                logging.warning(f"Difference in key '{k}' for '{v['id']}': '{left}' vs '{right}'")

        # Get the page contents
        contents = v.get('contents')
        if not contents:
            logging.warning(f"Wiki page '{v['id']}' missing 'contents'")
            continue

        # Update the wiki page data
        w['contents'] = contents
        w['_merged'] = True

    # Print all pages that have missing data after load
    missing = [v['id'] for v in wiki_page_versions.values() if '_merged' not in v]
    if missing:
        logging.warning(f"Missing wiki contents data for {missing}")

    logging.info(f"    Found {count} wiki page entries")


def wikicommitgenerator(wikiversions, order):
    """
    A generator producing a dict of git commits data containing wiki edits
    """

    # Collect all the latest current versions of the wiki pages
    pages = {}
    missing_authors = set()

    for v in sorted(wikiversions, key=lambda v: v['_updated_at']):
        p = v['_wiki_page']
        now = v['_updated_at']

        # Make ordered list of wiki pages that are present at this time
        indexpages = filter(lambda w: w['_created_at'] <= now and w['status'] == 1, order)

        fname = 'pages/' + p['page_name'] + '.md'
        author = v['_user']

        # Warn if we don't have the data for the user
        if v['user_id'] not in missing_authors and (not author.get('name') or not author.get('email')):
            logging.warning(f"Missing name or email for user '{v['user_id']}'")
            missing_authors.add(v['user_id'])

        pages[fname] = v['contents'] or None

        yield {
            'name': p['page_name'],
            'version': p['version'],
            'files': {
                'readme.md': wikiindexproducer(indexpages),
                fname: v['contents'] or None,
            },
            'author_name': nameorid(author),
            'author_email': author.get('email', WIKI_UNKNOWN_EMAIL),
            'message': v['change_comment'] or '',
            'date': now,
            'latest': v['version'] == p['version'],
        }

    # Convert the repo to GitHub format
    page_names = set(v['page_name'] for v in order)
    files = {}
    for k, v in pages.items():
        if not v:
            continue
        logging.debug(f"Migrating page '{k}'")
        contents = v#migratetexttomd(v, k, migrate_at=True, wikipages=page_names)
        if contents == v:
            continue
        files[k] = contents

    if files:
        yield {
            'name': 'ALL',
            'version': None,
            'pages': pages,
            'files': files,
            'author_name': WIKI_FIXUP_AUTHOR_NAME,
            'author_email': WIKI_FIXUP_AUTHOR_EMAIL,
            'message': WIKI_FIXUP_MESSAGE,
            'date': datetime.now().replace(microsecond=0),
            'latest': True,
        }


def wikiindexproducer(index):
    """ Produce the index menu """

    out = f'''{WIKI_MENU_HEADING}

'''
    for v in index:
        out += ('  ' * v['_level']) + f"* [{v['page_name']}](pages/{v['page_name']}.md)\n"
    return out


def scrapeusers(data):
    """
    Find all users reference in all tables
    """

    # Copy the predefined user database
    users = {k: v.copy() for k, v in ASSEMBLA_USERID.items()}

    for table, entries in data.items():
        if table.startswith('_'):
            continue
        for v in entries:
            for t in ('user_id', 'created_by', 'updated_by', 'reporter_id', 'assigned_to_id'):
                if t in v:
                    uid = v[t]
                    if not uid:
                        continue
                    u = users.setdefault(uid, {})
                    u.setdefault('id', uid)
                    u.setdefault('tables', set())
                    u['tables'].add(table)

    return users


def mergeuserdata(userdata, users):
    """
    Merge incoming user data with the main data dict
    :param userdata: imported user data from file fetched with userdump
    :param users: dict of all users which the imported data will update
    """

    count = 0
    for v in userdata:
        count += 1
        w = users.get(v['id'])
        if not w:
            logging.warning(f"Skipping user '{v['id']}'. Not mentioned in main dump file")
            continue

        # The redacted emails in file will interfere with preset emails. Its better to remove
        # it altogether
        if v.get('email') == 'name@domain':
            del v['email']

        w.update(v)
        w['_merged'] = True

    missing = [v['id'] for v in users.values() if '_merged' not in v]
    if missing:
        logging.warning(f"Missing user data for {missing}")

    logging.info(f"    Found {count} user entries")


# To find old lists. Variants:
#   # List
RE_LIST = re.compile(r'^# (.*)$', re.M)

# To find '** line'
RE_LIST2 = re.compile(r'^\*\*([^\*]*)$', re.M)

# To find '** line'
RE_LIST3 = re.compile(r'^([ \t]*)\*\*([^\*]*)$', re.M)

# To find old headers. Variants:
#   .h1 Title  .h2 Title
RE_HEADING = re.compile(r'^h(\d). ', re.M)

# To find !text!. Variants:
#    !<link!
# 1=pre indent, 2=text
RE_IMAGE = re.compile(r'(^[ \t]+)?!<?(\S+)!', re.M)

# To find @text@
RE_QUOTE = re.compile(r'@([^@\n]+?)@', re.M)

# To find <pre><code> blocks
RE_PRECODE = re.compile(r'<pre><code>(.*?)</code></pre>', re.M | re.S)

# To find <pre>...</pre> or {{{ ... }}} blocks
RE_PRE = re.compile(r'(<pre>.*?</pre>|{{{.*?}}})', re.M | re.S)

# To find table headers (|_. col1 |_. col2 |_. ... |)
# 1=pre indent, 2=headers excluding opening '|_.' and closing '|'
RE_TABLEHEADER = re.compile(r'^([ \t]+)?\|_\.(.*?)\|[ \t]*$', re.M)

# To find table headers (||= col1 =||= col2 =||)
# 1=pre indent, 2=headers excluding opening '|_.' and closing '|'
RE_TABLEHEADER2 = re.compile(r'^([ \t]+)?\|\|=(.*?)=\|\|[ \t]*$', re.M)

def sub_tableheader(m):
    """ Substitute table header format """
    columns = m[2].split('|_.')
    return f'| {" | ".join([c.strip() for c in columns])} |\n|{" --- |" * len(columns)}'

def sub_tableheader2(m):
    """ Substitute table header format """
    columns = m[2].split('=||=')
    return f'| {" | ".join([c.strip() for c in columns])} |\n|{" --- |" * len(columns)}'

# Find whole table (indicated by lines of |something|)
RE_TABLE = re.compile(r'(^[ \t]*\|.*\|[ \t]*$\n)+', re.M)

def sub_tableaddheader(m):
    """ Ensure table has header """
    m0 = m[0].replace('||', '|')
    if '| --- |' in m0:
        return m0
    lines = m0.split('\n')
    columns = len(lines[0].split('|')) - 2
    return f'|{" |"*columns}\n|{" --- |"*columns}\n{m0}'

# To find [[links]]. Variants:
#   [[Wiki]]  [[Wiki|Printed name]]  [[url:someurl]]  [[url:someurl|Printed name]]
# 1=pre indent, 2=prefix:, 3=first group, 4=| second group, 5=second group
RE_LINK = re.compile(r'(^[ \t]+)?\[\[(\w+?:)?(.+?)(\|(.+?))?\]\]', re.M)

def sub_link(m, ref, is_wiki, wikipages, documents):
    """ Subsitute [[link]] blocks with MD links """
    m3 = m[3]
    if not m[2]:
        # Is a [[wiki]] link (no prefix:)
        m5 = m[5]
        if not wikipages or m3 not in wikipages:
            logging.warning(f"{ref}: Wiki links to unknown page '{m3.strip()}'")
        if is_wiki:
            if not m5:
                # Bare wiki link
                return f"[[{m3}]]"
            # Wiki link with name
            return f"[[{m5.strip()}|{m3}]]"
        else:
            if not m5:
                m5 = m3
            return f"[{m5.strip()}](../wiki/{m3})"
    if m[2] == 'url:':
        # Plain link without name
        if not m[5]:
            return m3
        m5 = m[5].strip()
        if m3 == m5:
            return m3
        # Named link
        return f"[{m5}]({m3})"
    if m[2] in ('file:', 'image:'):
        what = 'attachment' if m[2] == 'file:' else 'image'
        # Reference to file attachment
        doc = {}
        if documents:
            doc = documents.get(m3)
        if not doc:
            logging.warning(f"{ref}: Reference to unknown {what} '{m3}'")
        else:
            m3 = doc['filename']
            logging.info(f"{ref}: Inserting reference to {what} '{m3}'")
        return f"[{what.capitalize()} {m3}]({m3})"
    if m[2] == 'r:':
        # Git reference
        return m3
    if m[2] in ('http:', 'https:'):
        return f"[{m[5].strip()}]({m[2]}{m3})"
    # Fallthrough
    logging.warning(f"{ref}: Unparseable link '{m[0]}'")
    return f"[[{m[2] or ''}{m3 or ''}{m[4] or ''}]]"

# To find (name)[link]
RE_LINK2 = re.compile(r'\[(.*?)\]\((.*?)\)', re.M)

def sub_link2(m):
    """ Subsitute (name)[link] blocks with indentical links """
    m1 = m[1].strip()
    m2 = m[2].strip()
    if m1 == m2:
        return m1
    return m[0]

# To find [[links]] on separate lines
RE_LINK3 = re.compile(r'^\[\[.*\]\]$', re.M)

# To find URLs
RE_URL = re.compile(r'\bhttps?://([\w\.\-]+)(/[\w\.\-/%#]*)?(\?[\w=%&\.\-$]*)?')

def sub_url(m, is_wiki):
    """ Replace URLs listed in URL_RE_REPLACE list """
    t = m[0]
    for r, n in (_URL_RE_WIKI if is_wiki else _URL_RE_TICKETS) + _URL_RE:
        t = r.sub(n, t)
    return t


# TODO:
#  - Ticket #202.1
#    Table def: ||= host api =||= status=||
#    Interpretation: | --- | --- | --- | --- | --- |
def migratetexttomd(text, ref, migrate_at=False, is_wiki=True, wikipages=None, documents=None):
    if not text:
        return text

    # Convert to unix line endings
    otext = text
    text = "\n".join(text.splitlines())
    if otext[-1] == '\n' or text[-1] != '\n':
        text += '\n'

    # Split on all <pre> groups
    textlist = []
    for text in RE_PRE.split(text):

        # The text is a <pre>...</pre> group
        if text.startswith('<pre>') or text.startswith('{{{'):

            if text.startswith('{{{') and text.endswith('}}}'):
                text = text[3:-3]
            if text.startswith('<pre>') and text.endswith('</pre>'):
                text = text[5:-6]
            if text.startswith('<code>') and text.endswith('</code>'):
                text = text[6:-7]

            if '\n' in text:
                lines = text.split('\n')
                pre = '\n' if lines[0] else ''
                post = '\n' if lines[-1] and not lines[-1].startswith('>') else ''
                text = "```" + pre + text + post + "```"
            else:
                text = "`" + text + "`"

            textlist.append(text)
            continue

        # Not a <pre>..</pre> group, i.e. ordinary wiki:

        # Replace # lines with numbered lists
        text = RE_LIST.sub(lambda m: '1. ' + m[1], text)

        # Replace '** line' with '  * line'
        text = RE_LIST2.sub(lambda m: '   * ' + m[1], text)

        # Replace '   ** line' with '   * line'
        text = RE_LIST3.sub(lambda m: m[1] + '* ' + m[2], text)

        # Replacing .h1 .h2 headers
        text = RE_HEADING.sub(lambda m: '#' * int(m[1]) + ' ', text)

        # Replacing !image!
        text = RE_IMAGE.sub(lambda m: f'![{m[2].split("/")[-1]}]({m[2]})', text)

        # Replacing @quote@
        if migrate_at:
            text = RE_QUOTE.sub(lambda m: f'`{m[1]}`', text)

        # Replacing <hr>
        text = text.replace('<hr>', '---')

        # Replacing [[links]]
        text = RE_LINK.sub(functools.partial(sub_link, ref=ref, is_wiki=is_wiki, wikipages=wikipages, documents=documents), text)

        # Replacing [name](link)
        text = RE_LINK2.sub(sub_link2, text)

        # Insert a newline on lines with [[links]] to ensure its not inline text
        text = RE_LINK3.sub(lambda m: '\n' + m[0], text)

        # Commit segment
        textlist.append(text)

    # Combine into continous text again
    text = ''.join(textlist)

    # Replace table headers for |_. headers |_.
    text = RE_TABLEHEADER.sub(sub_tableheader, text)

    # Replace table headers for ||= headers =||
    text = RE_TABLEHEADER2.sub(sub_tableheader2, text)

    # Ensure tables have table headers
    text = RE_TABLE.sub(sub_tableaddheader, text)

    # Replace URLs in text
    text = RE_URL.sub(functools.partial(sub_url, is_wiki=is_wiki), text)

    # Inform about remaining assembla links
    for m in RE_URL.finditer(text):
        if 'assembla' not in m[1]:
            continue
        logging.warning(f"{ref}: Link to {colorama.Fore.GREEN}Assembla{colorama.Style.RESET_ALL}: '{m[0]}'")

    return text


def dumpdict(filename, files, prefix):
    """ Dump the content of dict 'files' into a textfile """
    with open(filename, 'wb') as f:
        for k, v in files.items():
            if v:
                # Convert to unix line endings
                v = "\n".join(v.splitlines())
                t = f'\n{"_"*80}\n{prefix}{k}\n{v}\n'
                f.write(t.encode())


def ticketparser(data):
    """
    Parse the tickets
    """

    # milestones
    # ==========
    #   basecamp_milestone_id, budget, completed_date, created_at, created_by, description,
    #   due_date, from_basecamp, id, is_completed, obstacles, planner_type, project_plan_type,
    #   project_plan_url, release_level, release_notes, space_id, start_date, title, updated_at,
    #   updated_by, user_id

    # DEBUG
    # printtable(data['milestones'], exclude=('space_id', ))

    # ticket_statuses
    # ===============
    #    created_at, id, list_order, name, settings, space_tool_id, state, updated_at

    # DEBUG
    # printtable(data['ticket_statuses'], include=('_label', ))

    # tag_names
    # =========
    #    color, created_at, id, name, space_id, state, updated_at

    # DEBUG
    # printtable(data['tag_names'], include=('_label', ))

    # ticket_tags
    # ===========
    #   created_at, id, tag_name_id, ticket_id, updated_at, user_id
    for v in data['ticket_tags']:
        v['_tag_name'] = data.find('tag_names', v['tag_name_id'])

    # workflow_property_vals
    # ======================
    #   id, space_tool_id, value, workflow_instance_id, workflow_property_def_id
    for v in data['workflow_property_vals']:
        v['_type'] = data.find('workflow_property_defs', v['workflow_property_def_id'])['title']

    # workflow_property_defs
    # ======================
    #   autosort, created_at, default_value, flags, hide, id, order, required,
    #   space_tool_id, support_tool_permission, title, type, updated_at,
    #   without_default_on_form

    # workflow_property_def_settings
    # ==============================
    #   created_at, id, option, workflow_property_def_id

    # DEBUG
    # printtable(data['ticket_changes'], include=('_label', '_before', '_after'), filter=lambda x: x['subject'] == 'milestone_id')

    # ticket_comments
    # ===============
    #    comment, created_on, id, rendered, ticket_changes, ticket_id, updated_at, user_id
    for v in data['ticket_comments']:
        v['_created_on'] = datetime.fromisoformat(v['created_on'])
        v['_updated_at'] = datetime.fromisoformat(v['updated_at'])

        changes = list(filter(lambda x: x['ticket_comment_id'] == v['id'], data['ticket_changes']))
        v['_changes'] = changes
        for c in changes:
            c['_comment'] = v

        v['_user'] = data.find('_users', v['user_id'])

    # DEBUG
    # printtable(data['ticket_comments'], exclude=('comment', ), include=('_changes',))

    # tickets
    # =======
    #   assigned_to_id, completed_date, component_id, created_on, description, due_date, estimate,
    #   id, importance, is_story, milestone_id, milestone_updated_at, notification_list, number,
    #   permission_type, priority, reporter_id, space_id, state, status_updated_at, story_importance,
    #   summary, ticket_status_id, total_estimate, total_invested_hours, total_working_hours,
    #   updated_at, working_hours
    for v in data['tickets']:
        ticket = v['id']

        v['_created_on'] = datetime.fromisoformat(v['created_on'])
        if not v['state']:
            v['_completed_date'] = datetime.fromisoformat(v['completed_date'])
        v['_updated_at'] = datetime.fromisoformat(v['updated_at'])
        v['_milestone'] = data.find('milestones', v['milestone_id'], default=None)
        v['_milestone_text'] = dig(v, '_milestone', 'title')
        v['_ticket_status'] = data.find('ticket_statuses', v['ticket_status_id'])
        v['_reporter'] = data.find('_users', v['reporter_id'])
        v['_assigned_to'] = data.find('_users', v['assigned_to_id'], default=None)
        v['_state'] = githubstate(v['state'])
        v['_priority'] = ASSEMBLA_PRIORITY_MAPPING[v['priority']]
        v['_status'] = v['_ticket_status']['name']
        v['_tags'] = set([
            x['_tag_name']['name'] for x in data['ticket_tags']
            if x['ticket_id'] == ticket
        ]) or None

        comments = list(filter(lambda x: x['ticket_id'] == ticket, data['ticket_comments']))
        v['_comments'] = comments
        for c in comments:
            c['_ticket'] = v

        # Set component and keywords
        for wf in filter(lambda x: x['workflow_instance_id'] == ticket, data['workflow_property_vals']):
            t = wf['_type'].lower()
            if t == 'keywords':
                # Split keywords into distinct words
                s = v.setdefault('_' + t, set())
                s.update(wf['value'].split(' '))
            elif t == 'component':
                s = v.setdefault('_' + t, set())
                s.add(wf['value'])
            else:
                logging.warning(f"Unknown workflow name '{t}' on ticket {v['id']}")

    # ticket_changes
    # ===============
    #   after, before, created_at, extras, id, subject, ticket_comment_id, updated_at

    def _notify(name, field, v):
        logging.warning(f"Uknown {name} '{v[field]}' on ticket change {v['id']} in ticket #{v['_comment']['_ticket']['number']}")

    for v in data['ticket_changes']:
        v['_created_at'] = datetime.fromisoformat(v['created_at'])
        v['_updated_at'] = datetime.fromisoformat(v['updated_at'])

        subject = v['subject']
        if subject == 'status':
            v['_before'] = findfirst(lambda x: x['name'] == v['before'], data['ticket_statuses'], None)
            v['_after'] = findfirst(lambda x: x['name'] == v['after'], data['ticket_statuses'])
            if v['before'] and not v['_before']:
                _notify('ticket status', 'before', v)
            if v['after'] and not v['_after']:
                _notify('ticket status', 'after', v)

        elif subject == 'milestone_id':
            v['_before'] = findfirst(lambda x: x['title'] == v['before'], data['milestones'])
            v['_after'] = findfirst(lambda x: x['title'] == v['after'], data['milestones'])
            if v['before'] and not v['_before']:
                _notify('milestone', 'before', v)
            if v['after'] and not v['_after']:
                _notify('milestone', 'after', v)

        elif subject == 'assigned_to_id':
            v['_before'] = findfirst(lambda x: x.get('login', Unset) == v['before'], data['_users'])
            v['_after'] = findfirst(lambda x: x.get('login', Unset) == v['after'], data['_users'])
            if v['before'] and not v['_before']:
                _notify('user', 'before', v)
            if v['after'] and not v['_after']:
                _notify('user', 'after', v)

    # Ensure all issues are present in order. Otherwise the migration to GitHub will be out of sync with Assembla
    tickets = set(k['number'] for k in data['tickets'])
    for i in range(1, len(data['tickets']) + 1):
        if i not in tickets:
            logging.warning(f"   Assembla ticket #{i} missing, injecting dummy issue")
            data['tickets'].append({
                'number': i,
                'summary': 'Dummy issue',
                'description': '',
                'state': 0,
                '_reporter': ASSEMBLA_USERID['dummy'],
                '_created_on': datetime.now().replace(microsecond=0),
                '_updated_at': datetime.now().replace(microsecond=0),
                '_completed_date': datetime.now().replace(microsecond=0),
                '_state': 'closed',
                '_status': None,
                '_comments': [],
            })

    # DEBUG
    # printtable(data['tickets'],
    #            exclude=('description', 'summary', 'space_id', 'component_id', 'working_hours', 'is_story', 'notification_list',
    #                     'total_invested_hours', 'total_working_hours', 'estimate', 'total_estimate', 'story_importance', 'due_date',
    #                     'permission_type'),
    #            include=('_state', '_milestone_text', '_labels'))


class ChangeRecord:
    """ Helper class for tracking the initial value when setting new values
        (where the old value is known)
    """

    def __init__(self, final, initial=None):
        """ final is a dict containing the final values of the timeline """
        if not initial:
            initial = {}
        self.initial = {k: initial.get(k, Unset) for k in final}
        self.current = {k: Unset for k in final}
        self.final = final.copy()

    def set(self, group, insert, remove):
        """ Set a new value 'insert' in 'group'. The old value 'remove' is
            removed
        """
        # If the inital value is unset use the removed value is the initial value
        if self.initial[group] is Unset:
            self.initial[group] = remove
        self.current[group] = insert

    def getinitial(self):
        """ Return a dict of the calculated initial values """
        first = {}
        for k in self.current:
            if self.initial[k] is not Unset:
                v = self.initial[k]
            elif self.current[k] is not Unset:
                v = self.current[k]
            else:
                v = self.final[k]
            first[k] = v
        return {k: v for k, v in first.items() if v is not Unset}

    def notfinal(self):
        """ Return a dict of values which is not equal the final value """
        return {k: v for k, v in self.current.items() if v is not Unset and v != self.final[k]}


def tickettimelinegenerator(ticket):
    """
    Return a list of changes to the current ticket
    :param ticket: dict containing the ticket data
    """

    # Get the final state values from the ticket data
    final = {
        'state': ticket['_state'],
        'milestone': dig(ticket, '_milestone', 'title'),
        'assignee': ticket.get('_assigned_to'),
        'priority': ticket.get('_priority'),
        'status': ticket['_status'],
        'tags': ticket.get('_tags', set()) or set(),
        'keywords': ticket.get('_keywords', set()) or set(),
        'component': ticket.get('_component', set()) or set(),
    }

    # Setup a timeline object. As there is no info in Assembla about what the
    # state was at the start, the TimelineRecord() keeps track of all encountered
    # changes and use this to reconstruct backwards the original state.
    # final is the known final value.
    timeline = ChangeRecord(final=final, initial={
        'state': 'open',
        'status': 'New',
    })

    # Setup the first create issue change, which will be updated later with
    # the deduced initial state
    first = {
        'user': ticket['_reporter'],
        'date': ticket['_created_on'],
    }
    changes = [first]

    lastclose = {}
    for v in ticket['_comments']:
        changedata = {}
        params = set()

        if not v['comment'] and not v['_changes']:
            # logging.warning(f"Ticket #{ticket['number']}: No changes in issue. Skipping.")
            continue

        # Issue comment - Append the comment as a separate change
        if v['comment']:
            changedata.update({
                'body': v['comment'],
                'user': v['_user'],
                'date': v['_created_on'],
            })
            changes.append(changedata)
            changedata = {}

        # Check for changes
        for c in v['_changes']:

            subject = c['subject']
            if subject == 'status':
                before = githubstate(c['_before']['state'])
                after = githubstate(c['_after']['state'])
                timeline.set('state', after, before)
                timeline.set('status', c['_after']['name'], c['_before']['name'])
                params.update(('state', 'status'))
                continue

            elif subject == 'milestone_id':
                before = dig(c, '_before', 'title')
                after = dig(c, '_after', 'title')
                timeline.set('milestone', after, before)
                params.add('milestone')
                continue

            elif subject == 'assigned_to_id':
                timeline.set('assignee', c['_after'], c['_before'])
                params.add('assignee')
                continue

            elif subject == 'priority':
                timeline.set('priority', c['after'], c['before'])
                params.add('priority')
                continue

            elif subject in ('tags', 'Component', 'Keywords'):
                subject = subject.lower()
                before = set() if not c['before'] else set(c['before'].split(','))
                after = set() if not c['after'] else set(c['after'].split(','))
                timeline.set(subject, after, before)
                params.add(subject)
                continue

            elif subject == 'attachment':
                after = c['after'].split('\n')
                # logging.info(f"Ticket #{ticket['number']}: Ticket has attachment {after}")
                continue

            # Ignored changes
            elif subject in (
                    'permission_type', 'CommentContent', 'description', 'summary',
                    'milestone_updated_at', 'total_invested_hours', 'Estimate',
                    'Sum of Child Estimates', 'attachment_updated:filesize'):
                continue

            logging.warning(f"Ticket #{ticket['number']}: Unknown change '{c['subject']}'")

        # Setup the change and append it
        if params:
            changedata.update({
                'values': timeline.current.copy(),  # Return a full copy of the current state values
                'params': params,                   # With params indicating which fields have changed
                'user': v['_user'],
                'date': v['_created_on'],
            })
            changes.append(changedata)

            # Save the last close event
            if changedata.get('state') == 'closed':
                lastclose = changedata

    # Verify that the completed date on the ticket matches the change history (within 20 second slack)
    if lastclose:
        delta = lastclose.get('date') - ticket.get('_completed_date')
        if abs(delta.total_seconds()) > 20:
            logging.warning(f"Ticket #{ticket['number']}: Ticket close date does not match change history. Time difference: {abs(delta)}")

    # Update the first edit entry with the computed starting values
    initial = timeline.getinitial()
    first.update({
        'values': initial,
        'params': set(initial.keys()),
    })

    # Iterate over the stored current state values to replace any Unset objects with the
    # initial value for that parameter
    for change in changes:
        if 'values' not in change:
            continue
        values = change['values']
        for k, v in values.items():
            if v is Unset:
                values[k] = initial[k]

    # If the current state does not match the ticket data, the change history is incomplete and
    # must be updated with the final data from the ticket
    notfinal = timeline.notfinal()
    if notfinal:
        expect = {k: timeline.final[k] for k in notfinal}
        logging.warning(f"Ticket #{ticket['number']}: Change history inconsistency in {notfinal}. Expected final value {expect}")

        # Last step: Edit issue (often closes)
        # notfinal['type'] = 'edit'
        # changes.append(notfinal)

    return changes


def tickettogithub(ticket, changes, wikipages=None, documents=None):
    """
    Convert ticket with changes list to github format
    """
    github = {}
    key = ticket['number']

    # Conversion to labels
    labels = set(flatten([
        ASSEMBLA_TO_GITHUB_LABELS['status'].get(ticket['_status']),
        ASSEMBLA_TO_GITHUB_LABELS['priority'].get(ticket.get('_priority')),
        [ASSEMBLA_TO_GITHUB_LABELS['tags'].get(t) for t in ticket.get('tags', [])],
        [ASSEMBLA_TO_GITHUB_LABELS['keywords'].get(t) for t in ticket.get('_keywords', [])],
        [ASSEMBLA_TO_GITHUB_LABELS['component'].get(t) for t in ticket.get('_component', [])],
    ]))

    # Create the github issue object
    github = {
        # Description
        "title": ticket['summary'],
        "body": migratetexttomd(ticket['description'], f'Ticket #{key}', is_wiki=False, wikipages=wikipages, documents=documents),
        "annotation": githubcreatedheader(ticket['_reporter']),

        # Dates
        "created_at": githubtime(ticket['_created_on']),
        "updated_at": githubtime(ticket['_updated_at']),
        "closed_at": githubtime(ticket.get('_completed_date')),

        # Users
        "reporter": githubuser(ticket.get('_reporter')),
        "assignee": githubuser(ticket.get('_assigned_to')),

        # Meta fields
        "milestone": dig(ticket, '_milestone', 'title'),
        "closed": not ticket['state'],
        "labels": labels,
    }

    # Iterate over the changes
    prev = {}
    ghchanges = []
    for i, change in enumerate(changes):
        ckey = f'{key}.{i}'

        # Create the change object for the github data
        ghchange = {
            "user": githubuser(change['user']),
            "date": githubtime(change['date']),
        }
        ghchanges.append(ghchange)

        # The change is a comment
        if change.get('body'):
            ghchange.update({
                "body": migratetexttomd(change.get('body'), f'Ticket #{ckey}', is_wiki=False, wikipages=wikipages, documents=documents),
                "annotation": githubcommentedheader(change['user']),
            })

        # The change is an edit of issue meta-data
        values = change.get('values', {}).copy()
        if values:
            labels = set(flatten([
                ASSEMBLA_TO_GITHUB_LABELS['status'].get(values['status']),
                ASSEMBLA_TO_GITHUB_LABELS['priority'].get(values['priority']),
                [ASSEMBLA_TO_GITHUB_LABELS['tags'].get(t) for t in values['tags'] or []],
                [ASSEMBLA_TO_GITHUB_LABELS['keywords'].get(t) for t in values['keywords']],
                [ASSEMBLA_TO_GITHUB_LABELS['component'].get(t) for t in values['component']],
            ]))

            # Generate the github state values
            ghvalues = {
                "labels": labels,
                "closed": values['state'] == 'closed',
                "milestone": values['milestone'],
                "assignee": githubuser(values['assignee']),
            }

            # Add them to the change. Indicate which fields have changed
            ghchange.update({
                "values": ghvalues,
                "params": set(k for k in ghvalues if prev.get(k) != ghvalues[k]),
            })

            # Set annotation text when issue is opening or closing
            if 'closed' in prev:
                if not prev['closed'] and ghvalues['closed']:
                    ghchange["annotation"] = githubeditedheader(change['user'], edit='closed')
                if prev['closed'] and not ghvalues['closed']:
                    ghchange["annotation"] = githubeditedheader(change['user'], edit='reopened')

            prev = ghvalues

    return (github, ghchanges)


def check_config(config, parser, required):

    missing = [
        k for k in required
        if k not in config or not config[k] or (config[k].startswith('**') and config[k].endswith('**'))
    ]
    if missing:
        parser.error(f"Missing config file fields: {' '.join(missing)}")


def check_authconfig(auth, parser, required):

    # Ensure we have auth data and the fields needed
    if not auth:
        parser.error("Authentication config --auth is required")
    missing = [
        k for k in required
        if k not in auth or not auth[k] or (auth[k].startswith('**') and auth[k].endswith('**'))
    ]
    if missing:
        parser.error(f"Missing auth file fields: {' '.join(missing)}")


class ColorFormatter(logging.Formatter):
    """ Logger for formatting colored console output """
    def format(self, record):
        # Replace the original format with one customized by logging level
        self._style._fmt = {
            logging.ERROR: f'{colorama.Fore.RED}%(levelname)s:{colorama.Style.RESET_ALL} %(msg)s',
            logging.WARNING: f'{colorama.Fore.YELLOW}%(levelname)s:{colorama.Style.RESET_ALL} %(msg)s',
        }.get(record.levelno, '%(levelname)s: %(msg)s')
        return super().format(record)


# -----------------------------------------------------------------------------
#  MAIN
#
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v', action="count", default=0, help='verbose logging')
    parser.add_argument('--config', '-c', metavar="JSON", help="Configuration file")
    parser.add_argument('--auth', '-a', metavar="JSON", help='Authentication config')
    subparser = parser.add_subparsers(dest="command", required=True, title="command", help="Command to execute")

    subcmd = subparser.add_parser('dump', help="Dump assembla database tables")
    subcmd.add_argument('table', nargs='?', help="Table to dump")
    subcmd.add_argument('--headers', '-H', action="store_true", help="Dump header fields")
    subcmd.add_argument('--include', '-i', action="append", help="Fields to include")
    subcmd.add_argument('--exclude', '-x', action="append", help="Fields to exclude")
    subcmd.add_argument('--limit', '-l', type=int, help="Limit the number of lines")
    subcmd.set_defaults(func=cmd_dump)

    subcmd = subparser.add_parser('lstickets', help="List tickets")
    subcmd.add_argument('--quiet', '-q', action="store_true", help="Do not print tickets")
    subcmd.add_argument('--github', '-g', action="store_true", help="Show tickets after github field conversion")
    subcmd.add_argument('--description', '-d', action="store_true", help="Show description fields")
    subcmd.add_argument('--comments', '-c', action="store_true", help="Show comment fields")
    subcmd.add_argument('--content-before', '-B', required=False, help="Dump ticket contents before convert")
    subcmd.add_argument('--content-after', '-A', required=False, help="Dump ticket contents after convert")
    subcmd.add_argument('issue', nargs="*", help="Issue to print")
    subcmd.set_defaults(func=cmd_lstickets)

    subcmd = subparser.add_parser('lsusers', help="List users")
    subcmd.add_argument('--table', '-t', action="append", help="Show only users from this table")
    subcmd.set_defaults(func=cmd_lsusers)

    subcmd = subparser.add_parser('lswiki', help="List wiki pages")
    subcmd.add_argument('--quiet', '-q', action="store_true", help="Do not print tickets")
    subcmd.add_argument('--changes', '-c', action="store_true", help="Show page changes")
    subcmd.add_argument('--content', '-d', action="store_true", help="Show page content")
    subcmd.add_argument('--tables', '-t', action="store_true", help="Show as tables")
    subcmd.add_argument('--content-before', '-B', required=False, help="Dump wiki contents before convert")
    subcmd.add_argument('--content-after', '-A', required=False, help="Dump wiki contents after convert")
    subcmd.set_defaults(func=cmd_lswiki)

    subcmd = subparser.add_parser('ticketsconvert', help="Convert tickets to GitHub repo")
    subcmd.add_argument('--dry-run', '-n', action="store_true", help="Only check the data")
    subcmd.add_argument('--mk1', action="store_true", help="Use the old GitHub importer")
    subcmd.set_defaults(func=cmd_ticketsconvert)

    subcmd = subparser.add_parser('userscrape', help="Scrape users from Assembla")
    subcmd.add_argument('out', help="Output file to store users scrape")
    subcmd.set_defaults(func=cmd_userscrape)

    subcmd = subparser.add_parser('wikiconvert', help="Convert to GitHub wiki repo")
    subcmd.add_argument('dir', help="Working dir for wiki git repo")
    subcmd.add_argument('--dry-run', '-n', action="store_true", help="Do not commit any data")
    subcmd.add_argument('--no-convert', action="store_true", help="Do not commit markdown conversion changes")
    subcmd.set_defaults(func=cmd_wikiconvert)

    subcmd = subparser.add_parser('wikiscrape', help="Scrape wiki from Assembla")
    subcmd.add_argument('out', help="Output file to store wiki scrape")
    subcmd.set_defaults(func=cmd_wikiscrape)

    options = parser.parse_args()

    # -------------------------------------------------------------------------
    #  Logging

    # log to stdout
    logging_level = logging.DEBUG if options.verbose > 1 else logging.INFO
    root = logging.getLogger()
    root.setLevel(logging_level)
    channel = logging.StreamHandler(sys.stdout)
    channel.setLevel(logging_level)
    # channel.setFormatter(logging.Formatter('%(levelname)s:  %(message)s'))
    channel.setFormatter(ColorFormatter())
    root.addHandler(channel)

    logging.info(f"Running assembla2github.py v{TOOLVERSION} ({TOOLDATE})")

    # -------------------------------------------------------------------------
    #  Read config file

    config = {
        'parser': parser,
        'options': options,
    }
    configfile = options.config
    if not configfile and pathlib.Path("config.json").exists():
        configfile = 'config.json'
    if configfile:
        logging.info(f"Reading configuration from '{configfile}'")
        with open(configfile, 'r') as f:
            config = json.load(f)

    # Check for required config fields
    check_config(config, parser, ('dumpfile', ))

    # -------------------------------------------------------------------------
    #  Read auth file

    auth = {}
    authfile = options.auth
    if not authfile and pathlib.Path("auth.json").exists():
        authfile = 'auth.json'
    if authfile:
        logging.info(f"Reading authentication data from '{authfile}'")
        with open(authfile, 'r') as f:
            auth = json.load(f)
    config['auth'] = auth

    # -------------------------------------------------------------------------
    #  Read the dump file

    logging.info(f"Parsing dumpfile '{config['dumpfile']}'")
    with open(config['dumpfile'], encoding='utf8') as filereader:
        data = DictPlus()
        tablefields = {}

        # for each line determine the assembla object type, read all attributes to dict using the mappings
        # assign a key for each object which is used to link github <-> assembla objects to support updates
        for linenum, line, table, row in filereadertoassemblaobjectgenerator(filereader, tablefields):

            # Collect the file data
            data.setdefault(table, [])
            data.get(table).append(row)

        logging.info(f"    Parsed {linenum} lines")

    # -------------------------------------------------------------------------
    #  Index the data

    logging.info("Indexing the data")

    # Store the fields for the tables
    data['_fields'] = tablefields

    # Convert table list to dicts indexed by key using keymap
    data['_index'] = indexassembladata(data, {

        # None key specified index key for all unlisted tables.
        # None: 'id',

        # Tables to index
        'wiki_pages': 'id',
        'milestones': 'id',
        'ticket_statuses': 'id',
        'workflow_property_defs': 'id',
        'wiki_page_versions': 'id',
        'tag_names': 'id',
        'documents': 'id',
    })

    # -------------------------------------------------------------------------
    #  Read the wiki dump data

    if 'wikidump' in config:

        logging.info(f"Parsing wiki dumpfile '{config['wikidump']}'")

        with open(config['wikidump'], encoding='utf8') as filereader:
            wikidata = json.load(filereader)

        # Merge the file data with the main assembla database
        mergewikidata(wikidata, data['_index']['wiki_page_versions'])

    # -------------------------------------------------------------------------
    #  UserID scrape

    logging.info("Scraping for user IDs")

    users = scrapeusers(data)
    data["_index"]["_users"] = users
    data["_users"] = list(users.values())

    # -------------------------------------------------------------------------
    #  Read the user dump data

    if 'userdump' in config:

        logging.info(f"Parsing user dumpfile '{config['userdump']}'")

        with open(config['userdump'], encoding='utf8') as filereader:
            userdata = json.load(filereader)

        # Merge the file data with the main assembla database
        mergeuserdata(userdata, data['_index']['_users'])

    # -------------------------------------------------------------------------
    # Initialize the URL replace regexps

    if 'repo' in config:
        space = data["spaces"][0]["name"].lower()
        url = "https://github.com/" + config['repo']

        def replace(inlist, outlist):
            for k in inlist:
                k0 = k[0].replace('{ASSEMBLA_SPACE}', space)
                k1 = k[1].replace('{GITHUB_URL}', url)
                outlist.append((re.compile(k0), k1))

        global _URL_RE, _URL_RE_WIKI, _URL_RE_TICKETS
        replace(URL_RE_REPLACE, _URL_RE)
        replace(URL_RE_REPLACE_WIKI, _URL_RE_WIKI)
        replace(URL_RE_REPLACE_TICKETS, _URL_RE_TICKETS)

    # -------------------------------------------------------------------------
    # Run the command

    # Set the verbosity
    logging_level = logging.DEBUG if options.verbose else logging.INFO
    root.setLevel(logging_level)
    channel.setLevel(logging_level)

    logging.info(f"Executing command '{options.command}'")
    options.func(parser, options, config, auth, data)


# -----------------------------------------------------------------------------
#  Dump table command
def cmd_dump(parser, options, config, auth, data):

    if not options.table:

        tables = sorted(data.keys())
        if options.headers:
            print("Assembla table fields:")
            headers = [
                {
                    'table': t,
                    'fields': sorted(data['_fields'].get(t, [])),
                }
                for t in tables
            ]
            printtable(headers)
            return

        print("Assembla tables:")
        printtable([{'table': t} for t in tables])
        return

    table = data.get(options.table)
    if not table:
        parser.error(f"No such table: '{options.table}'")

    srange = None
    if options.limit:
        srange = slice(0, options.limit)

    print(f"Table '{options.table}':")
    printtable(table, include=options.include, exclude=options.exclude, slice=srange)


# -----------------------------------------------------------------------------
#  Print users
def cmd_lsusers(parser, options, config, auth, data):
    users = data["_index"]["_users"]
    tables = set(options.table or [])
    if options.table:
        logging.info(f"Showing users present in tables: {' '.join(tables)}")
        users = list(filter(lambda v: any(v['tables'].intersection(tables)), users.values()))

    printtable(users, exclude=('tables', ))


# -----------------------------------------------------------------------------
#  User scrape from Assembla
def cmd_userscrape(parser, options, config, auth, data):

    # Check for required auth fields
    check_authconfig(auth, parser, ('assembla_key', 'assembla_secret'))

    headers = {
        'X-Api-Key': auth['assembla_key'],
        'X-Api-Secret': auth['assembla_secret'],
    }

    # Fetch all user info
    out = []
    for v in data["_index"]["_users"].values():

        # Brute force to ensure to not hit any rate limits
        time.sleep(0.1)

        logging.info(f"Fetching user '{v['id']}'")

        req = requests.get(
            f"https://api.assembla.com/v1/users/{v['id']}.json",
            headers=headers,
        )
        if req.status_code != 200:
            logging.error(f"   Failed to fetch: Error code {req.status_code}")
            continue
        jsdata = req.json()

        out.append(jsdata)

    # Save the entries to disk
    logging.info(f"Saving user data in '{options.out}'")
    with open(options.out, 'w') as f:
        json.dump(out, f)


# -----------------------------------------------------------------------------
#  List wiki pages
def cmd_lswiki(parser, options, config, auth, data):

    tprint = print
    if options.quiet or options.tables:
        tprint = lambda *a: None

    # Parse the wiki entries (making rich additions to objects in data) and
    # return the order of wiki pages
    wikiorder = wikiparser(data)

    # Iterate over each wiki page version in order from old to new and get
    # the data required for git commit
    for commit in wikicommitgenerator(data['wiki_page_versions'], wikiorder):

        if not commit['latest'] and not options.changes:
            continue

        tprint(f"""Wiki page {commit['name']}, Revision {commit['version']}
          Date    : {commit['date']}
          Author  : {commit['author_name']} <{commit['author_email']}>
          Message : {commit['message']}""")

        for f, content in commit['files'].items():
            ftext = f"{f}   MISSING DATA" if not content else f"{f}   {len(content)} bytes"
            pdata = ''
            if options.content:
                pdata = f'''
{"_"*120}
{content}
{"_"*120}'''
            tprint(f"            {ftext}{pdata}")

        tprint()

        # The 'ALL' is the last entry where the pages have been converted to GitHub markdown
        if commit['name'] == 'ALL':

            # Dump wiki pages to files (for comparisons)
            keys = list(commit['pages'].keys())
            if options.content_before:
                dumpdict(options.content_before, {k: commit['pages'].get(k) for k in keys}, 'Page ')
            if options.content_after:
                dumpdict(options.content_after, {k: commit['files'].get(k) for k in keys}, 'Page ')

    # Print the wiki data
    if options.tables and not options.quiet:
        if not options.changes:
            printtable(wikiorder, exclude=('space_id', 'contents'))
        else:
            printtable(data['wiki_page_versions'], exclude=('contents',))


# -----------------------------------------------------------------------------
#  WIKI scrape from Assembla
def cmd_wikiscrape(parser, options, config, auth, data):

    # Check for required auth fields
    check_authconfig(auth, parser, ('assembla_key', 'assembla_secret'))

    headers = {
        'X-Api-Key': auth['assembla_key'],
        'X-Api-Secret': auth['assembla_secret'],
    }

    # Parse the wiki entries (making rich additions to objects in data) and
    # return the order of wiki pages
    wikiorder = wikiparser(data)

    # Fetch all wiki pages
    out = []
    for v in wikiorder:

        # Brute force to ensure to not hit any rate limits
        time.sleep(0.1)

        logging.info(f"Fetching wiki page '{v['page_name']}'")

        req = requests.get(
            f"https://api.assembla.com/v1/spaces/{v['space_id']}/wiki_pages/{v['id']}/versions.json?per_page=40",
            headers=headers,
        )
        if req.status_code != 200:
            logging.error(f"   Failed to fetch: Error code {req.status_code}")
            continue
        jsdata = req.json()

        out.append(jsdata)

    # Save the entries to disk
    logging.info(f"Saving wiki scrape data in '{options.out}'")
    with open(options.out, 'w') as f:
        json.dump(out, f)


# -----------------------------------------------------------------------------
#  WIKI conversion
def cmd_wikiconvert(parser, options, config, auth, data):

    # Check for required config fields
    check_config(config, parser, ('repo', ))

    # Check for required auth fields
    # check_authconfig(auth, parser, ('username', 'password'))

    wikidir = pathlib.Path(options.dir)
    wikirepo = wikidir

    # Open git repo
    repo = None
    if not options.dry_run:

        url = 'https://github.com/' + config['repo'] + '.git'
        logging.info(f"Checking out '{url}'")

        repo = git.Repo.clone_from(url, wikidir)
        wikirepo = pathlib.Path(repo.working_tree_dir)

    # Parse the wiki entries (making rich additions to objects in data) and
    # return the order of wiki pages
    wikiorder = wikiparser(data)

    # DEBUG
    # printtable(wikiorder, include=('_level', ))

    # Iterate over each wiki page version in order from old to new and get
    # the data required for git commit
    for commit in wikicommitgenerator(data['wiki_page_versions'], wikiorder):

        name = f"{commit['name']}:{commit['version']}"
        logging.debug(f"Converting page '{name}'")

        pathlib.Path(wikirepo,'pages').mkdir(parents=True, exist_ok=True)

        files = []
        for name, contents in commit['files'].items():
            if not contents:
                logging.warning(f"Missing page data for {name}")
                continue
            fname = pathlib.Path(wikirepo, name)
            fname.write_bytes(contents.encode())
            files.append(str(fname))

        # Add the files
        if repo:
            repo.index.add(files)

        actor = git.Actor(commit['author_name'], commit['author_email'])
        date = commit['date'].astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        # The 'ALL' is the last entry where the pages have been converted to GitHub markdown
        if commit['name'] == 'ALL':

            # Skip commit of convert if --no-convert is used
            if options.no_convert:
                continue

        # Commit the changes
        if repo:
            repo.index.commit(
                commit['message'],
                author=actor,
                author_date=date,
                committer=actor,
                commit_date=date,
            )

    logging.info(f"Conversion complete. '{wikidir}' contains converted Wiki repo. Please review and push")


# -----------------------------------------------------------------------------
#  List tickets
def cmd_lstickets(parser, options, config, auth, data):

    tprint = print
    if options.quiet:
        tprint = lambda *a: None

    # Parse the wiki entries to get the wiki page names
    wikipages = set(v['page_name'] for v in wikiparser(data))

    # Prep the dataset for conversion
    ticketparser(data)

    before = {}
    after = {}

    for ticket in sorted(data['tickets'], key=lambda v: v['number']):

        if options.issue and str(ticket['number']) not in options.issue:
            continue

        # Save the description before conversion
        before[f"#{ticket['number']} Description"] = ticket['description']

        # Get the timeline for the ticket
        changes = tickettimelinegenerator(ticket)

        # Save the comments before conversion
        for i, change in enumerate(changes):
            if change.get('body'):
                before[f"#{ticket['number']} Comment {i}"] = change['body']

        # Convert the issue to github data
        issue, ghchanges = tickettogithub(ticket, changes, wikipages=wikipages, documents=data['_index']['documents'])

        # Save the description after conversion
        after[f"#{ticket['number']} Description"] = issue['body']

        # Save the comments after conversion
        for i, change in enumerate(ghchanges):
            if change.get('body'):
                after[f"#{ticket['number']} Comment {i}"] = change['body']

        if options.github:
            changes = ghchanges
            body = issue['body']
        else:
            body = ticket['description']

        description = ''
        if options.description:
            description = f'''
{"_"*120}
{body.rstrip()}
{"_"*120}'''

        if options.github:
            tprint(f"""#{ticket['number']}  {issue['title']}
          Description : {len(issue['body'])} bytes{description}
          Closed      : {issue['closed']}
          Reporter    : {issue['reporter']}
          Assignee    : {issue['assignee']}
          Created     : {issue['created_at']}
          Closed at   : {issue['closed_at']}
          Updated at  : {issue['updated_at']}
          Milestone   : {issue['milestone']}
          Labels      : {issue['labels']}
""")
        else:
            tprint(f"""#{ticket['number']}  {ticket['summary']}
          Description : {len(ticket['description'])} bytes{description}
          Status      : {ticket['_status']}  ({ticket['_state']})
          Reporter    : {nameorid(ticket['_reporter'])}
          Assignee    : {nameorid(ticket.get('_assigned_to'))}
          Created     : {ticket.get('created_on')}
          Closed      : {ticket.get('completed_date')}
          Milestone   : {dig(ticket, '_milestone', 'title')}
          Priority    : {ticket.get('_priority')}
          Keywords    : {ticket.get('_keywords')}
          Component   : {ticket.get('_component')}
          Tags        : {ticket.get('_tags')}
""")

        for i, change in enumerate(changes):

            user = change['user']
            if not options.github:
                user = nameorid(user)

            if change.get('body'):
                tprint(f"            ({i})  COMMENT  {change['date']}  {user}")

            if change.get('params'):
                op = 'CREATE' if i == 0 else 'CHANGE'
                tprint(f"            ({i})  {op:7s}  {change['date']}  {user}")

            if i == 0 and options.github:
                change['annotation'] = issue['annotation']
            if 'annotation' in change:
                tprint(f"                   {colorama.Fore.CYAN}{'Annotation':10s}{colorama.Style.RESET_ALL} : {change['annotation']}")

            if change.get('body'):
                comment = ''
                if options.comments:
                    comment = f'''
{"_"*120}
{change['body'].rstrip()}
{"_"*120}'''
                tprint(f"                   {'Comment':10s} : {len(change['body'])} bytes{comment}")

            values = change.get('values', {})
            params = change.get('params', set())
            # if params:
            #    print(f"                   {'Params':10s} : {params}")

            for k in params:
                v = values[k]
                if k == 'state':
                    continue
                if k == 'assignee' and not options.github:
                    v = nameorid(values['assignee'])
                if k == 'status':
                    v = f"{v}  ({values['state']})"
                tprint(f"                   {k.capitalize():10s} : {v}")

        tprint()

    # Dump ticket comments to files (for comparisons)
    if options.content_before:
        dumpdict(options.content_before, before, 'Ticket ')
    if options.content_after:
        dumpdict(options.content_after, after, 'Ticket ')


# -----------------------------------------------------------------------------
#  Tickets conversion
def cmd_ticketsconvert(parser, options, config, auth, data):

    # Check for required config fields
    check_config(config, parser, ('repo', ))

    # Check for required auth fields
    check_authconfig(auth, parser, ('username', 'password'))

    # Parse the wiki entries to get the wiki page names
    wikipages = set(v['page_name'] for v in wikiparser(data))

    # Prep the dataset for conversion
    ticketparser(data)

    # establish github connection
    repo = None
    if not options.dry_run:
        ghub = github.Github(auth['username'], auth['password'])
        repo = ghub.get_repo(config['repo'])

    # -------------------------------------------------------------------------
    #  MILESTONES

    github_milestones = []

    if repo:
        github_milestones = list(repo.get_milestones(state='all'))
        # print(github_milestones)

    logging.info('Converting milestones -> milestones...')
    for assemblamilestone in data['milestones']:
        title = assemblamilestone['title']
        githubmilestone = findfirst(lambda v: v.title == title, github_milestones)
        if githubmilestone:
            logging.info(f"    Skipping existing milestone '{title}'")
            continue

        req = {
            'title': title,
            'state': githubstate(assemblamilestone['is_completed']),
            'description': assemblamilestone['description'],
            'due_on': datetime.fromisoformat(assemblamilestone['due_date']),
        }

        if repo:
            logging.info(f"    Creating milestone: '{title}'")
            repo.create_milestone(**req)

    if repo:
        github_milestones = list(repo.get_milestones(state='all'))
        # print(github_milestones)

    # -------------------------------------------------------------------------
    #  LABELS

    github_labels = []

    if repo:
        github_labels = list(repo.get_labels())
        # print(github_labels)

    logging.info('Converting ticket statuses and tags -> labels...')
    for label in NEW_GITHUB_LABELS:
        githublabel = findfirst(lambda v: v.name == label, github_labels)
        if githublabel:
            logging.info(f"    Skipping exiting label '{label}'")
            continue

        req = {
            'name': label,
            'color': NEW_GITHUB_LABELS[label],
        }

        if repo:
            logging.info(f"    Creating label: '{label}'")
            repo.create_label(**req)

    if repo:
        github_labels = list(repo.get_labels())
        # print(github_labels)

    # -------------------------------------------------------------------------
    #  ISSUES

    github_issues = []

    if repo:
        github_issues = list(repo.get_issues())

    logging.info('Converting tickets -> issues...')

    tick = 0
    for ticket in sorted(data['tickets'], key=lambda v: v['number']):
        key = ticket['number']
        logging.debug(f"{colorama.Fore.GREEN}Ticket #{key}{colorama.Style.RESET_ALL}")

        if repo:
            githubissue = findfirst(lambda v: v.number == key, github_issues)
            if githubissue:
                logging.info(f"    Skipping existing issue {key}")
                continue

        # Get the timeline changes for the ticket
        changes = tickettimelinegenerator(ticket)

        # Convert the issue to github data
        ghissue, ghchanges = tickettogithub(ticket, changes, wikipages=wikipages, documents=data['_index']['documents'])

        # Find the GH milestone
        milestone = ghissue['milestone']
        ghmilestone = findfirst(lambda v: v.title == milestone, github_milestones)
        if ghmilestone:
            ghmilestone = ghmilestone.number

        #   "issue": {
        #     "title": "Imported from some other system",
        #     "body": "...",
        #     "created_at": "2014-01-01T12:34:58Z",
        #     "closed_at": "2014-01-02T12:24:56Z",
        #     "updated_at": "2014-01-03T11:34:53Z",
        #     "assignee": "jonmagic",
        #     "milestone": 1,
        #     "closed": true,
        #     "labels": [
        #       "bug",
        #       "low"
        #     ]
        #   },
        issue = {
            'title': ghissue['title'],
            'body': ghissue['annotation'] + '\n\n' + ghissue['body'],
            'created_at': ghissue['created_at'],
            'updated_at': ghissue['updated_at'],
            'assignee': None,  # Don't want to migrate assignee
            'milestone': ghmilestone,
            'closed': ghissue['closed'],
            'labels': list(ghissue['labels']),
        }
        if ghissue['closed']:
            issue['closed_at'] = ghissue['closed_at']

        #    "comments": [
        #    {
        #      "created_at": "2014-01-02T12:34:56Z",
        #      "body": "talk talk"
        #    }
        #    ]
        comments = []
        for change in ghchanges:

            if 'annotation' not in change:
                continue

            body = ''
            if change.get('body'):
                body = '\n\n' + change.get('body')

            comments.append({
                'created_at': change['date'],
                'body': change['annotation'] + body,
            })

        # Setup GitHub POST request data
        url = f"https://api.github.com/repos/{config['repo']}/import/issues"
        gauth = (auth['username'], auth['password'])
        headers = {
            'Accept': 'application/vnd.github.golden-comet-preview+json'
        }
        jdata = {
            'issue': issue,
            'comments': comments,
        }

        if repo:

            logging.info(f"  Uploading ticket #{key}")

            # Post the issue data
            res = requests.post(url, json=jdata, auth=gauth, headers=headers)
            resjson = res.json()

            # print(f"    URL:     {url}")
            # print(f"    RETURN:  {res.status_code}")
            # print(f"    HEADERS: {res.headers}")
            # print(f"    JSON:    {resjson}")

            if res.status_code != 202:
                logging.error(f"Failed to upload ticket #{key}. Status code {res.status_code} returned")
                if 'message' in resjson:
                    logging.error(f"Response text: {resjson['message']}")
                break

            # Make sure that we have enough rate limits requests remaining
            remain = int(res.headers.get('X-RateLimit-Remaining', '0'))
            reset = datetime.fromtimestamp(int(res.headers['X-RateLimit-Reset']), timezone.utc).astimezone()
            if time.time() > tick + 60:
                logging.info(f"  Remaining ratelimit quota: {remain} (will reset at {str(reset)})")
                tick = time.time()
            if remain < 100:
                logging.error(f"Rate limits exceeded. Aborting conversion. Please retry after {str(reset)}")
                return

            # Poll GitHub to get the issue ID
            delay = POLL_INITIAL
            failcount = 0
            print("   ", end='')
            while resjson['status'] == 'pending':

                # Sleep an exponential amount of time
                time.sleep(delay)
                delay = min(delay * POLL_FACTOR, POLL_MAX_DELAY)

                # Fetch the current status
                print(".", end='')
                url = resjson['url']
                res = requests.get(url, auth=gauth, headers=headers)

                try:
                    resjson = res.json()
                    jsonfail = False
                except json.decoder.JSONDecodeError as err:
                    resjson = {}
                    jsonfail = str(err)

                # print(f"    URL:     {url}")
                # print(f"    RETURN:  {res.status_code}")
                # print(f"    HEADERS: {res.headers}")
                # print(f"    JSON:    {resjson}")

                if res.status_code != 200 or jsonfail:
                    print('F', end='')
                    logging.error(f"Failed to get status of ticket #{key}. Status code {res.status_code} returned")
                    logging.error(f"Headers: {res.headers}")
                    if jsonfail:
                        logging.error(f"Could not load JSON: {jsonfail}")
                    if 'message' in resjson:
                        logging.error(f"Response text: {resjson['message']}")

                    # Ensure retries
                    failcount += 1
                    if failcount < POLL_MAX_FAILS:
                        logging.warning("Retrying...")
                        continue
                    break

            if res.status_code != 200:
                break

            # Get the github issue number and compare it against the expected ticket number
            issueid = resjson['issue_url'].replace(resjson['repository_url'] + '/issues/', '')
            print(f'  done, issue #{issueid}')

            if int(issueid) != ticket['number']:
                logging.error(f"Did not get equal issue id from GitHub. Got issue {issueid} for Assembla ticket {ticket['number']}")


if __name__ == "__main__":
    main()
