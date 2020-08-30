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

# Name of the GitHub repo
GITHUB_REPO = "<user>/<repo>"
GITHUB_URL = f'https://github.com/{GITHUB_REPO}'

# Map Assembla field values to GitHub lables. The value 'None' indicates that
# the field will be omitted.
ASSEMBLA_TO_GITHUB_LABELS = {
    'status': {
        'New': None,
        'Accepted': None,
        'Test': None,
        'Invalid': 'invalid',
        'Fixed': None,
        'Duplicate': 'duplicate',
        'WontFix': 'wontfix',
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
        'osx': 'osx',
        'linux': 'linux',
        'docs': 'documentation',
        'windows': 'windows',
        'git': 'git',
        'qa': 'test-qa',
    },
    'component': {
        'bindings': 'bindings',
        'build-systems': 'build',
        'common': 'src-common',
        'documentation': 'documentation',
        'global': 'audit',
        'host-api-alsa': ('src-alsa', 'linux'),
        'host-api-asihpi': 'src-asihpi',
        'host-api-asio': ('src-asio', 'win'),
        'host-api-coreaudio': ('src-coreaudio', 'osx'),
        'host-api-dsound': ('src-dsound', 'win'),
        'host-api-jack': ('src-jack'),
        'host-api-oss': ('src-oss'),
        'host-api-wasapi': ('src-wasapi', 'win'),
        'host-api-wdmks': ('src-wdmks', 'win'),
        'host-api-wmme': ('src-wmme', 'win'),
        'os-mac_osx': ('src-os-mac_osx', 'osx'),
        'os-windows': ('src-os-win', 'win'),
        'other': None,
        'public-api': 'public-api',
        'test': 'test',
        'website': 'website',
    },
    'keywords': {
        # NB the Assembla ticket database has been updated so that all relevant
        # tickets have been assigned a component. therefore there are only a few
        # workflow related keywords that need to be mapped
        'LIST-REVIEW': 'LIST-REVIEW',
        'IN-REVIEW': 'IN-REVIEW',
        'STARTER': 'good first issue',
        'STARTER-PLUS': 'good first issue',
        'QA': 'QA',
        'CMake': 'build-cmake',
        'git': 'git',
        'portmixer': 'portmixer'
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
    # Default GitHub tags that we will use (for status etc)
    'documentation': '0075ca',  # ('component' Blue)
    'duplicate': 'cfd3d7',  # (Grey)
    'enhancement': 'a2eeef',  # (Cyan) -- not used yet
    'good first issue': '7057ff',  # (Purple)
    'help wanted': '008672',  # (Dark Teal) -- not used yet
    'invalid': 'e4e669',  # (Mustard)
    'question': 'd876e3',  # (Fuchsia) -- not used and we will be bouncing questions to the mailing list
    'wontfix': 'ffffff',  # (White)

    # components ('component' Blue)
    # These are subsystems or areas that usually have separate maintainers such as APIs
    'bindings': '0075ca',
    'bindings-cpp': '0075ca',  # new
    'bindings-java': '0075ca',  # new
    'build': '0075ca',
    'build-cmake': '0075ca',  # new
    'build-autoconf': '0075ca',  # new
    'build-msvs': '0075ca',  # new
    'src-common': '0075ca',
    # 'documentation': '0075ca', (already added above)
    'audit': '0075ca',  # was 'global' (used for code review, design review)
    'src-alsa': '0075ca',
    'src-asihpi': '0075ca',
    'src-asio': '0075ca',
    'src-coreaudio': '0075ca',
    'src-dsound': '0075ca',
    'src-jack': '0075ca',
    'src-oss': '0075ca',
    'src-wasapi': '0075ca',
    'src-wdmks': '0075ca',
    'src-wmme': '0075ca',
    'src-os-mac_osx': '0075ca',
    'src-os-win': '0075ca',
    'public-api': '0075ca',
    'test': '0075ca',  # tests in /tests
    'test-qa': '0075ca',  # new: semi-automated tests in /qa
    'test-examples': '0075ca',  # new: tests aka examples in /examples
    'website': '0075ca',

    # priority
    'P0': 'ff0000',  # Critical / Show Stopper (Red)
    'P1': 'ff7f27',  # Highest (Orange)
    'P2': 'fff200',  # High (Yellow)
    'P3': '7fff00',  # Normal (Green)
    'P4': '00a2e8',  # Low (Blue)
    'P5': 'e5e5e5',  # Lowest (Grey)

    # tags (lighter than component blue)
    # Mostly just operating system and some misceleny
    # os
    'osx': '80caff',
    'linux': '80caff',
    'windows': '80caff',
    # misc
    'git': '80caff',

    # keywords
    'LIST-REVIEW': 'cc3399',  # (Deep pink)
    'IN-REVIEW': 'cc3399',  # ditto
    'STARTER': '22b14c',    # (Emerald Green)
    'STARTER-PLUS': '22b14c',  # ditto
    'QA': '2d73eb',
    'portmixer': '80caff',
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
WIKI_FIXUP_AUTHOR_NAME = "Wiki converter"
WIKI_FIXUP_AUTHOR_EMAIL = "none@localhost"
WIKI_FIXUP_MESSAGE = "Updated Wiki to GitHub formatting"
WIKI_UNKNOWN_EMAIL = "none@localhost"

# URLs to replace when converting Wiki
_GITHUB_URL_REPLACE = [
    (r'^https?://(www|app)\.assembla\.com/spaces/portaudio/tickets/(\d+)$', r'#\2'),
    (r'^https?://www\.assembla\.com/spaces/portaudio/tickets$', f'{GITHUB_URL}/issues'),
    (r'^https?://www\.assembla\.com/spaces/portaudio/tickets\.$', f'{GITHUB_URL}/issues.'),
    (r'^https?://www\.assembla\.com/spaces/portaudio/tickets/new$', f'{GITHUB_URL}/issues/new'),
    (r'^https?://www\.assembla\.com/spaces/portaudio/wiki$', f'{GITHUB_URL}/wiki'),
    (r'^https?://(www|app)\.assembla\.com/spaces/portaudio/wiki/(.*)$', r'[[\2]]'),
    (r'^https?://www\.assembla\.com/spaces/portaudio/milestones$', f'{GITHUB_URL}/milestones'),
    (r'^https?://www\.assembla\.com/spaces/portaudio/git/source$', f'{GITHUB_URL}/'),
    (r'^https?://(www|app)\.assembla\.com/spaces/portaudio/git/source/([^\?]*)(\?.*)?$', f'{GITHUB_URL}/tree/\\2'),
    (r'^https?://app\.assembla\.com/spaces/portaudio/git/commits/list$', f'{GITHUB_URL}/commits'),
    (r'^https?://app\.assembla\.com/spaces/portaudio/git/commits/(\w+)$', r'\1'),
    (r'^https?://git\.assembla\.com/portaudio\.git$', f'{GITHUB_URL}.git'),
]
GITHUB_URL_REPLACE = [(re.compile(x[0]), x[1]) for x in _GITHUB_URL_REPLACE]


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
    return user.get('name', user.get('id'))


def githubuser(user):
    # First try to return '@<githubusername>'
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
        return f"Issue created by {name}{dt}"
    return f"Issue created by {name} on Assembla{dt}"


def githubcommentedheader(user, date=None):
    dt = f' at {date}' if date else ''
    name = githubuser(user)
    if name.startswith('@'):
        return f"Comment by {name}{dt}"
    return f"Comment by {name} on Assembla{dt}"


def githubeditedheader(user, date=None, edit='edited'):
    dt = f' at {date}' if date else ''
    name = githubuser(user)
    if name.startswith('@'):
        return f"Issue {edit} by @{name}{dt}"
    return f"Issue {edit} by {name} on Assembla{dt}"


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
        line = ''.join(x for x in _line if x in string.printable)
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

        fname = p['page_name'] + '.md'
        author = v['_user']

        # Warn if we don't have the data for the user
        if v['user_id'] not in missing_authors and (not author.get('name') or not author.get('email')):
            logging.warning(f"Missing name or email for user '{v['user_id']}'")
            missing_authors.add(v['user_id'])

        pages[fname] = v['contents'] or None

        yield {
            'name': p['page_name'] + ':' + str(v['version']),
            'files': {
                '_Sidebar.md': wikiindexproducer(indexpages),
                fname: v['contents'] or None,
            },
            'author_name': nameorid(author),
            'author_email': author.get('email', WIKI_UNKNOWN_EMAIL),
            'message': v['change_comment'] or '',
            'date': now,
        }

    # Convert the repo to GitHub format
    page_names = set(v['page_name'] for v in order)
    files = {}
    for k, v in pages.items():
        if not v:
            continue
        logging.debug(f"Migrating page '{k}'")
        contents = migratetexttomd(v, k, page_names, migrate_at=True)
        if contents == v:
            continue
        files[k] = contents

    if files:
        yield {
            'name': 'ALL',
            'pages': pages,
            'files': files,
            'author_name': WIKI_FIXUP_AUTHOR_NAME,
            'author_email': WIKI_FIXUP_AUTHOR_EMAIL,
            'message': WIKI_FIXUP_MESSAGE,
            'date': datetime.now().replace(microsecond=0),
        }


def wikiindexproducer(index):
    """ Produce the index menu """

    out = '''# PortAudio

'''
    for v in index:
        out += ('  ' * v['_level']) + f"* [[{v['page_name']}]]\n"
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
RE_TABLEHEADER = re.compile(r'^([ \t]+)?\|_\.(.*?)\|\s*$', re.M)

def sub_tableheader(m):
    """ Substitute table header format """
    columns = m[2].split('|_.')
    return f'| {" | ".join([c.strip() for c in columns])} |\n|{" --- |" * len(columns)}'

# Find whole table (indicated by lines of |something|)
RE_TABLE = re.compile(r'(^[ \t]*\|.*\|[ \t]*$\n)+', re.M)

def sub_tableaddheader(m):
    """ Ensure table has header """
    if '| --- |' in m[0]:
        return m[0]
    lines = m[0].split('\n')
    columns = len(lines[0].split('|')) - 2
    return f'|{" |"*columns}\n|{" --- |"*columns}\n{m[0]}'

# To find [[links]]. Variants:
#   [[Wiki]]  [[Wiki|Printed name]]  [[url:someurl]]  [[url:someurl|Printed name]]
# 1=pre indent, 2=prefix:, 3=first group, 4=| second group, 5=second group
RE_LINK = re.compile(r'(^[ \t]+)?\[\[(\w+?:)?(.+?)(\|(.+?))?\]\]', re.M)

def sub_link(m, page_names, ref):
    """ Subsitute [[link]] blocks with MD links """
    m3 = m[3]
    if not m[2]:
        # Is a wiki link (no prefix:)
        # Special fixups
        if m3 == 'tips/index':
            m3 = 'Tips'
        if m3 == 'platforms/index':
            m3 = 'Platforms'
        if not page_names or m3 not in page_names:
            logging.warning(f"{ref}: Wiki links to unknown page '{m3.strip()}'")
        if not m[5]:
            # Bare wiki link
            return f"[[{m3}]]"
        # Wiki link with name
        return f"[[{m[5].strip()}|{m3}]]"
    if m[2] == 'url:':
        # Plain link without name
        if not m[5]:
            return m3
        m5 = m[5].strip()
        if m3 == m5:
            return m3
        # Named link
        return f"[{m5}]({m3})"
    #if m[2] == 'file:':
    #    # Reference to file attachment
    #    logging.warning(f"{ref}: Reference to attachment '{m3}'")
    #    return f"[Attachment {m3}]({m3})"
    if m[2] == 'r:':
        # Git reference
        return m3
    if m[2] in ('http:', 'https:'):
        return f"[{m[5].strip()}]({m[2]}{m3})"
    # Fallthrough
    logging.warning(f"{ref}: Unparseable link '{m[0]}'")
    return f"[[{m[2] or ''}{m3 or ''}{m[4] or ''}]]"

# To find [[links]] on separate lines
RE_LINK2 = re.compile(r'^\[\[.*\]\]$', re.M)

# To find URLs
RE_URL = re.compile(r'\bhttps?://([\w\.\-]+)(/[\w\.\-/%#]*)?(\?[\w=%&\.\-$]*)?')

def sub_url(m):
    """ Replace URLs listed in GITHUB_URL_REPLACE list """
    t = m[0]
    for r, n in GITHUB_URL_REPLACE:
        t = r.sub(n, t)
    return t


# TODO:
#  - Ticket #202.1
#    Table def: ||= host api =||= status=||
#    Interpretation: | --- | --- | --- | --- | --- |
def migratetexttomd(text, ref, page_names=None, migrate_at=False):
    if not text:
        return text

    # Convert to unix line endings
    text = "\n".join(text.splitlines())

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
                post = '\n' if lines[-1] else ''
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
        text = RE_LINK.sub(functools.partial(sub_link, ref=ref, page_names=page_names), text)

        # Insert a newline on lines with [[links]] to ensure its not inline text
        text = RE_LINK2.sub(lambda m: '\n' + m[0], text)

        # Commit segment
        textlist.append(text)

    # Combine into continous text again
    text = ''.join(textlist)

    # Replace table headers
    text = RE_TABLEHEADER.sub(sub_tableheader, text)

    # Ensure tables have table headers
    text = RE_TABLE.sub(sub_tableaddheader, text)

    # Replace URLs in text
    text = RE_URL.sub(sub_url, text)

    # Inform about remaining assembla links
    for m in RE_URL.finditer(text):
        if 'assembla' not in m[1]:
            continue
        logging.warning(f"{ref}: Link to Assembla: '{m[0]}'")

    return text


def dumpfiles(filename, files, prefix):
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


class TimelineRecord:
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
        if isinstance(self.final[group], set):
            # If the inital value is unset the removed value is the initial value
            if self.initial[group] is Unset:
                self.initial[group] = set([remove])
            v = self.current[group]
            if v is Unset:
                v = set([insert])
            else:
                v.remove(remove)
                v.add(insert)
        else:
            # If the inital value is unset the removed value is the initial value
            if self.initial[group] is Unset:
                self.initial[group] = remove
            v = insert
        self.current[group] = v

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
        'tags': ticket.get('_tags', set()),
        'keywords': ticket.get('_keywords'),
        'component': ticket.get('_component'),
    }

    # Setup a timeline object. As there is no info in Assembla about what the
    # state was at the start, the TimelineRecord() keeps track of all encountered
    # changes and use this to reconstruct backwards the original state.
    # final is the known final value.
    timeline = TimelineRecord(final=final, initial={
        'state': 'open',
        'status': 'New',
    })

    # Setup the first create issue change
    first = {
        'title': ticket['summary'],
        'body': ticket['description'],
        'user': ticket['_reporter'],
        'date': ticket['_created_on'],
        'closed': ticket.get('_completed_date', None),
        'updated': ticket['_updated_at'],
    }
    changes = [first]

    lastclose = {}
    for v in ticket['_comments']:
        changedata = {}

        if not v['comment'] and not v['_changes']:
            # logging.warning(f"Ticket #{ticket['number']}: No changes in issue. Skipping.")
            continue

        # Issue comment
        if v['comment']:
            changedata.update({
                'body': v['comment']
            })

        # Check for changes
        for c in v['_changes']:

            subject = c['subject']
            if subject == 'status':
                before = githubstate(c['_before']['state'])
                after = githubstate(c['_after']['state'])
                timeline.set('state', after, before)
                timeline.set('status', c['_after']['name'], c['_before']['name'])
                changedata.update({
                    'state': after,
                    'status': c['_after']['name'],
                })
                continue

            elif subject == 'milestone_id':
                before = dig(c, '_before', 'title')
                after = dig(c, '_after', 'title')
                timeline.set('milestone', after, before)
                changedata.update({
                    'milestone': after,
                })
                continue

            elif subject == 'assigned_to_id':
                timeline.set('assignee', c['_after'], c['_before'])
                changedata.update({
                    'assignee': c['_after'],
                })
                continue

            elif subject in ('tags', 'Component', 'Keywords', 'priority'):
                subject = subject.lower()
                timeline.set(subject, c['after'] or None, c['before'] or None)
                changedata.update({
                    subject: c['after'],
                })
                continue

            elif subject == 'attachment':
                after = c['after'].split('\n')
                logging.info(f"Ticket #{ticket['number']}: Ticket has attachment {after}")
                continue

            # Ignored changes
            elif subject in (
                    'permission_type', 'CommentContent', 'description', 'summary',
                    'milestone_updated_at', 'total_invested_hours', 'Estimate',
                    'Sum of Child Estimates', 'attachment_updated:filesize'):
                continue

            logging.warning(f"Ticket #{ticket['number']}: Unknown change '{c['subject']}'")

        # Setup the change and append it
        if changedata:
            changedata.update({
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
    first.update({k: v for k, v in timeline.getinitial().items() if v})

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


def check_config(auth, parser, required):

    # Ensure we have auth data and the fields needed
    if not auth:
        parser.error("Authentication config --auth is required")
    missing = [
        k for k in required
        if k not in auth or not auth[k] or (auth[k].startswith('**') and auth[k].endswith('**'))
    ]
    if missing:
        parser.error(f"Missing auth fields: {' '.join(missing)}")


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
    parser.add_argument('--dumpfile', '-f', metavar="FILE", required=True, help='assembla dumpfile')
    parser.add_argument('--wikidump', '-w', metavar="FILE", help="wiki dumpfile")
    parser.add_argument('--userdump', '-u', metavar="FILE", help="user dumpfile")
    parser.add_argument('--auth', '-a', help='Authentication config')
    subparser = parser.add_subparsers(dest="command", required=True, title="command", help="Command to execute")

    subcmd = subparser.add_parser('dump', help="Dump assembla tables")
    subcmd.add_argument('table', nargs='?', help="Table to dump")
    subcmd.add_argument('--headers', action="store_true", help="Dump header fields")
    subcmd.add_argument('--include', '-i', action="append", help="Fields to include")
    subcmd.add_argument('--exclude', '-x', action="append", help="Fields to exclude")
    subcmd.add_argument('--limit', '-l', type=int, help="Limit the number of lines")
    subcmd.set_defaults(func=cmd_dump)

    subcmd = subparser.add_parser('lsusers', help="List users")
    subcmd.add_argument('--table', '-t', action="append", help="Show only users from this table")
    subcmd.set_defaults(func=cmd_lsusers)

    subcmd = subparser.add_parser('lswiki', help="List wiki pages")
    subcmd.add_argument('--changes', action="store_true", help="Show page changes")
    subcmd.set_defaults(func=cmd_lswiki)

    subcmd = subparser.add_parser('ticketsconvert', help="Convert tickets to GitHub repo")
    subcmd.add_argument('repo', help="GitHub repository")
    subcmd.add_argument('--dry-run', '-n', action="store_true", help="Only check the data")
    subcmd.add_argument('--mk1', action="store_true", help="Use the old GitHub importer")
    subcmd.add_argument('--content-before', '-B', required=False, help="Dump ticket contents before convert (for debug)")
    subcmd.add_argument('--content-after', '-A', required=False, help="Dump ticket contents after convert (for debug)")
    subcmd.set_defaults(func=cmd_tickets)

    subcmd = subparser.add_parser('userscrape', help="Scrape users from Assembla")
    subcmd.add_argument('dump', help="Output file to store users scrape")
    subcmd.set_defaults(func=cmd_userscrape)

    subcmd = subparser.add_parser('wikiconvert', help="Convert to GitHub wiki repo")
    subcmd.add_argument('repo', help="GitHub repository")
    subcmd.add_argument('--dry-run', '-n', action="store_true", help="Do not commit any data")
    subcmd.add_argument('--no-convert', action="store_true", help="Do not commit conversion change")
    subcmd.add_argument('--content-before', '-B', required=False, help="Dump wiki contents before convert (for debug)")
    subcmd.add_argument('--content-after', '-A', required=False, help="Dump wiki contents after convert (for debug)")
    subcmd.set_defaults(func=cmd_wikiconvert)

    subcmd = subparser.add_parser('wikiscrape', help="Scrape wiki from Assembla")
    subcmd.add_argument('dump', help="Output file to store wiki scrape")
    subcmd.set_defaults(func=cmd_wikiscrape)

    runoptions = parser.parse_args()

    # log to stdout
    logging_level = logging.DEBUG if runoptions.verbose > 1 else logging.INFO
    root = logging.getLogger()
    root.setLevel(logging_level)
    channel = logging.StreamHandler(sys.stdout)
    channel.setLevel(logging_level)
    # channel.setFormatter(logging.Formatter('%(levelname)s:  %(message)s'))
    channel.setFormatter(ColorFormatter())
    root.addHandler(channel)

    # -------------------------------------------------------------------------
    #  Read auth file

    auth = {}
    if runoptions.auth:
        logging.info(f"Reading authentication data from '{runoptions.auth}'")
        with open(runoptions.auth, 'r') as f:
            auth = json.load(f)

    # -------------------------------------------------------------------------
    #  Read the dump file

    logging.info(f"Parsing dumpfile '{runoptions.dumpfile}'")
    with open(runoptions.dumpfile, encoding='utf8') as filereader:
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
    })

    # -------------------------------------------------------------------------
    #  Read the wiki dump data

    if runoptions.wikidump:

        logging.info(f"Parsing wiki dumpfile '{runoptions.wikidump}'")

        with open(runoptions.wikidump, encoding='utf8') as filereader:
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

    if runoptions.userdump:

        logging.info(f"Parsing user dumpfile '{runoptions.userdump}'")

        with open(runoptions.userdump, encoding='utf8') as filereader:
            userdata = json.load(filereader)

        # Merge the file data with the main assembla database
        mergeuserdata(userdata, data['_index']['_users'])

    # -------------------------------------------------------------------------
    # Run the command

    # Set the verbosity
    logging_level = logging.DEBUG if runoptions.verbose else logging.INFO
    root.setLevel(logging_level)
    channel.setLevel(logging_level)

    logging.info(f"Executing command '{runoptions.command}'")
    runoptions.func(parser, runoptions, auth, data)


# -----------------------------------------------------------------------------
#  Dump table command
def cmd_dump(parser, runoptions, auth, data):

    if not runoptions.table:

        tables = sorted(data.keys())
        if runoptions.headers:
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

    table = data.get(runoptions.table)
    if not table:
        parser.error(f"No such table: '{runoptions.table}'")

    srange = None
    if runoptions.limit:
        srange = slice(0, runoptions.limit)

    print(f"Table '{runoptions.table}':")
    printtable(table, include=runoptions.include, exclude=runoptions.exclude, slice=srange)


# -----------------------------------------------------------------------------
#  Print users
def cmd_lsusers(parser, runoptions, auth, data):
    users = data["_index"]["_users"]
    tables = set(runoptions.table or [])
    if runoptions.table:
        logging.info(f"Showing users present in tables: {' '.join(tables)}")
        users = list(filter(lambda v: any(v['tables'].intersection(tables)), users.values()))

    printtable(users, exclude=('tables', ))


# -----------------------------------------------------------------------------
#  User scrape from Assembla
def cmd_userscrape(parser, runoptions, auth, data):

    # Check for required auth fields
    check_config(auth, parser, ('assembla_key', 'assembla_secret'))

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
    with open(runoptions.dump, 'w') as f:
        json.dump(out, f)


# -----------------------------------------------------------------------------
#  List wiki pages
def cmd_lswiki(parser, runoptions, auth, data):

    # Parse the wiki entries (making rich additions to objects in data) and
    # return the order of wiki pages
    wikiorder = wikiparser(data)

    if not runoptions.changes:
        printtable(wikiorder, exclude=('space_id', 'contents'))
    else:
        printtable(data['wiki_page_versions'], exclude=('contents',))


# -----------------------------------------------------------------------------
#  WIKI scrape from Assembla
def cmd_wikiscrape(parser, runoptions, auth, data):

    # Check for required auth fields
    check_config(auth, parser, ('assembla_key', 'assembla_secret'))

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
    logging.info(f"Saving wiki scrape data in '{runoptions.dump}'")
    with open(runoptions.dump, 'w') as f:
        json.dump(out, f)


# -----------------------------------------------------------------------------
#  WIKI conversion
def cmd_wikiconvert(parser, runoptions, auth, data):

    wikidir = pathlib.Path(runoptions.repo.split('/')[-1] + '.wiki')
    wikirepo = wikidir
    wikirepo.mkdir(exist_ok=True)

    # Open git repo
    repo = None
    if not runoptions.dry_run:
        repo = git.Repo.clone_from('https://github.com/' + runoptions.repo + '.wiki.git', wikirepo)
        wikirepo = pathlib.Path(repo.working_tree_dir)

    # Parse the wiki entries (making rich additions to objects in data) and
    # return the order of wiki pages
    wikiorder = wikiparser(data)

    # DEBUG
    # printtable(wikiorder, include=('_level', ))

    # Iterate over each wiki page version in order from old to new and get
    # the data required for git commit
    for commit in wikicommitgenerator(data['wiki_page_versions'], wikiorder):

        logging.debug(f"Converting page '{commit['name']}'")

        files = []
        for name, contents in commit['files'].items():
            if not contents:
                logging.warning(f"Missing page data for {commit['name']}")
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

            # Dump wiki pages to files (for comparisons)
            keys = list(commit['pages'].keys())
            if runoptions.content_before:
                dumpfiles(runoptions.content_before, {k: commit['pages'].get(k) for k in keys}, 'Page ')
            if runoptions.content_after:
                dumpfiles(runoptions.content_after, {k: commit['files'].get(k) for k in keys}, 'Page ')

            # Skip commit of convert if --no-convert is used
            if runoptions.no_convert:
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

    logging.info(f"Conversion complete. Remember to push the git repo in '{wikidir}'")


# -----------------------------------------------------------------------------
#  Tickets conversion
def cmd_tickets(parser, runoptions, auth, data):

    # Check for required auth fields
    check_config(auth, parser, ('username', 'password'))

    # Prep the dataset for conversion
    ticketparser(data)

    # establish github connection
    repo = None
    if not runoptions.dry_run:
        ghub = github.Github(auth['username'], auth['password'])
        repo = ghub.get_repo(runoptions.repo)

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

    if runoptions.mk1:
        github_import_mk1(parser, runoptions, auth, data, repo, github_issues, github_milestones)
    else:
        github_import_mk2(parser, runoptions, auth, data, repo, github_issues, github_milestones)


def github_import_mk1(parser, runoptions, auth, data, repo, github_issues, github_milestones):

    before = {}
    after = {}

    for ticket in sorted(data['tickets'], key=lambda v: v['number']):
        assemblakey = ticket['number']
        # logging.debug(f"{colorama.Fore.GREEN}Ticket #{assemblakey}{colorama.Style.RESET_ALL}")

        if repo:
            githubissue = findfirst(lambda v: v.number == assemblakey, github_issues)
            if githubissue:
                logging.info(f"    Skipping existing issue {assemblakey}")
                continue

        state = {
            'milestone': github.GithubObject.NotSet,
            'assignees': github.GithubObject.NotSet,
            'labels': github.GithubObject.NotSet,
        }

        for i, change in enumerate(tickettimelinegenerator(ticket), start=1):
            key = f"#{assemblakey}.{i}"
            # logging.debug(f"{colorama.Fore.MAGENTA}    {key}{colorama.Style.RESET_ALL}")

            body = change.get('body')
            if body:
                before[key] = body
                body = migratetexttomd(body, 'Ticket ' + key)
                after[key] = body

            # Convert to github values
            if 'assignee' in change:
                change['assignee'] = githubassignee(change['assignee'], assemblakey)
            if 'milestone' in change:
                milestone = findfirst(lambda v: v.title == change['milestone'], github_milestones)
                change['milestone'] = milestone or github.GithubObject.NotSet

            # Record the changes into the state record
            for param in ('state', 'priority', 'milestone', 'status', 'tags', 'keywords', 'component', 'assignee'):
                if param in change:
                    state[param] = change[param]

            # Compile the current label state
            prev = state['labels']
            state['labels'] = flatten([
                ASSEMBLA_TO_GITHUB_LABELS['status'].get(state.get('status')),
                ASSEMBLA_TO_GITHUB_LABELS['priority'].get(state.get('priority')),
                [ASSEMBLA_TO_GITHUB_LABELS['tags'].get(t) for t in state.get('tags', [])],
                [ASSEMBLA_TO_GITHUB_LABELS['keywords'].get(t) for t in state.get('keywords', [])],
                [ASSEMBLA_TO_GITHUB_LABELS['component'].get(t) for t in state.get('component', [])],
            ])
            if prev != state['labels']:
                change['labels'] = True

            def _rmbody(iter):
                d = iter.copy()
                if 'body' in d:
                    lines = d['body'].split('\n')
                    d['body'] = f"{lines[0]} + {len(lines)} lines, {len(d['body'])} bytes"
                return d

            if i == 1:

                req = dict(
                    title=change['title'],
                    body=githubcreatedheader(change['user'], change['date']) + '\n\n' + body,
                    labels=state.get('labels'),
                    milestone=state.get('milestone') or github.GithubObject.NotSet,
                    assignees=state.get('assignee') or github.GithubObject.NotSet,
                )
                logging.debug(f"{key} CREATE {_rmbody(req)}")

                if repo:
                    # create_issue(title, body=NotSet, assignee=NotSet, milestone=NotSet, labels=NotSet,
                    #              assignees=NotSet)
                    githubissue = repo.create_issue(**req)
                    if githubissue.number != assemblakey:
                        logging.error(f"GitHub created issue #{githubissue.number} for Assembla id #{assemblakey}")
                continue

            isedit = any([x in change for x in ('milestone', 'labels', 'assignee', 'state')])

            if body:
                req = dict(
                    body=githubcommentedheader(change['user'], change['date']) + '\n\n' + body,
                )
                logging.debug(f"{key} COMMENT {_rmbody(req)}")

                if repo:
                    # create_comment(body)
                    githubissue.create_comment(**req)

            if isedit:

                if not body:
                    req = dict(
                        body=githubeditedheader(change['user'], change['date']),
                    )
                    logging.debug(f"{key} COMMENT {_rmbody(req)}")

                    if repo:
                        # create_comment(body)
                        githubissue.create_comment(**req)

                req = dict(
                    labels=state.get('labels'),
                    milestone=state.get('milestone') or github.GithubObject.NotSet,
                    assignees=state.get('assignee') or github.GithubObject.NotSet,
                    state=state.get('state'),
                )
                logging.debug(f"{key} EDIT {_rmbody(req)}")

                if repo:
                    # edit(title=NotSet, body=NotSet, assignee=NotSet, state=NotSet, milestone=NotSet,
                    #      labels=NotSet, assignees=NotSet)
                    githubissue.edit(**req)

            # FIXME: Mundane rate limit
            if repo:
                time.sleep(0.1)

    # Dump ticket comments to files (for comparisons)
    if runoptions.content_before:
        dumpfiles(runoptions.content_before, before, 'Ticket ')
    if runoptions.content_after:
        dumpfiles(runoptions.content_after, after, 'Ticket ')


def github_import_mk2(parser, runoptions, auth, data, repo, github_issues, github_milestones):

    before = {}
    after = {}

    for ticket in sorted(data['tickets'], key=lambda v: v['number']):
        key = ticket['number']
        logging.debug(f"{colorama.Fore.GREEN}Ticket #{key}{colorama.Style.RESET_ALL}")

        if repo:
            githubissue = findfirst(lambda v: v.number == key, github_issues)
            if githubissue:
                logging.info(f"    Skipping existing issue {key}")
                continue

        body = ticket['description']
        if body:
            before[key] = body
            body = migratetexttomd(body, f'Ticket #{key}')
            after[key] = body

        milestone = dig(ticket, '_milestone', 'title')
        ghmilestone = findfirst(lambda v: v.title == milestone, github_milestones)
        if ghmilestone:
            ghmilestone = ghmilestone.number

        labels = flatten([
            ASSEMBLA_TO_GITHUB_LABELS['status'].get(ticket['_status']),
            ASSEMBLA_TO_GITHUB_LABELS['priority'].get(ticket['_status']),
            [ASSEMBLA_TO_GITHUB_LABELS['tags'].get(t) for t in ticket.get('tags', [])],
            [ASSEMBLA_TO_GITHUB_LABELS['keywords'].get(t) for t in ticket.get('_keywords', [])],
            [ASSEMBLA_TO_GITHUB_LABELS['component'].get(t) for t in ticket.get('_component', [])],
        ])

        closed = not ticket['state']

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
            'title': ticket['summary'],
            'body': githubcreatedheader(ticket['_reporter']) + '\n\n' + body,
            'created_at': githubtime(ticket['_created_on']),
            'updated_at': githubtime(ticket['_updated_at']),
            'assignee': None,  # githubassignee(ticket.get('_assigned_to'), key),
            'milestone': ghmilestone,
            'closed': closed,
            'labels': labels,
        }
        if closed:
            issue['closed_at'] = githubtime(ticket.get('_completed_date'))

        #    "comments": [
        #    {
        #      "created_at": "2014-01-02T12:34:56Z",
        #      "body": "talk talk"
        #    }
        #    ]
        comments = []
        n = 0
        for v in ticket['_comments']:
            if not v['comment']:
                continue
            n += 1
            ckey = f'{key}.{n}'

            astate = bstate = None
            for c in v['_changes']:
                if c['subject'] == 'status':
                    bstate = c['_before']['state']
                    astate = c['_after']['state']

            body = v['comment']
            if body:
                before[ckey] = body
                body = migratetexttomd(body, f'Ticket #{ckey}')
                after[ckey] = body

            comments.append({
                'body': githubcommentedheader(v['_user']) + '\n\n' + body,
                'created_at': githubtime(v['_created_on']),
            })

            if astate != bstate:
                if not astate and bstate:
                    action = 'closed'
                else:
                    action = 'reopened'
                comments.append({
                    'body': githubeditedheader(v['_user'], edit=action),
                    'created_at': githubtime(v['_created_on']),
                })

        url = f'https://api.github.com/repos/{runoptions.repo}/import/issues'
        gauth = (auth['username'], auth['password'])
        headers = {
            'Accept': 'application/vnd.github.golden-comet-preview+json'
        }
        jdata = {
            'issue': issue,
            'comments': comments,
        }

        if repo:
            res = requests.post(url, json=jdata, auth=gauth, headers=headers)
            if res.status_code != 202:
                pprint(jdata)
                print(f"RETURN:  {res.status_code}")
                print(f"HEADERS: {res.headers}")
                print(f"JSON:    {res.json()}")
                break

            print(f"   Remain: {res.headers['X-RateLimit-Remaining']}")
            time.sleep(1)

    # Dump ticket comments to files (for comparisons)
    if runoptions.content_before:
        dumpfiles(runoptions.content_before, before, 'Ticket ')
    if runoptions.content_after:
        dumpfiles(runoptions.content_after, after, 'Ticket ')


if __name__ == "__main__":
    main()
