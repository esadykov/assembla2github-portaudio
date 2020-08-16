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
import requests
import time
import github
import re
import itertools
import colorama

# Ensure colored output on win32 platforms
colorama.init()

# map your assembla ticket statuses to Open or Closed here.
ASSEMBLA_TICKET_STATUS_TO_GITHUB_ISSUE_STATUS = {
    'New': 'open',
    'Accepted': 'open',
    'Test': 'open',
    'Invalid': 'closed',
    'Fixed': 'closed',
    'Demo': 'closed',
    'Review / Estimation': 'open',
}

# map your assembla user hashes to github logins here.
ASSEMBLA_USERID_TO_GITHUB_USERID = {
    'XXX': 'User1',
    'YYY': 'User2',
    'ZZZ': 'User3',
}

# User mapping from assembla to github
#   - login: Assembla user name. Used to match tickets "assigned to" fields
#   - name: Presented name. Used for wiki git commits and tickets (if no github id exists)
#   - email: Used for wiki git commits
#   - github: GitHub user name. Used for tickets as @mentions
ASSEMBLA_USERID = {
}

# Settings for Wiki conversions
WIKI_FIXUP_AUTHOR_NAME = "Wiki converter"
WIKI_FIXUP_AUTHOR_EMAIL = "none@localhost"
WIKI_FIXUP_MESSAGE = "Updated Wiki to GitHub formatting"
WIKI_UNKNOWN_EMAIL = "none@localhost"

ASSEMBLA_MILESTONES = []
ASSEMBLA_TICKETS = []
ASSEMBLA_TICKET_STATUSES = []
ASSEMBLA_TICKET_COMMENTS = []
GITHUB_ISSUES = []
GITHUB_USERS = []
GITHUB_MILESTONES = []


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


def nameorid(user):
    """ Return the name or the id of the user """
    return user.get('name', user.get('id'))


def githubuser(user):
    """ Return the github user if present, otherwise return the name or id """
    if 'github' in user:
        return f"@{user['github']}"
    return user.get('name', user.get('id'))


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


def findgithubobjectbyassemblaid(assemblaid, githubobjectcollection):
    """
    :param assemblaid: the assembla id [#ID] assumed to be at the beginning of the title of the github object
    :param githubobjectcollection: the github objects to search
    :returns: return the first match or None
    """
    return next(iter(filter(lambda x: x.title.startswith(assemblaid), githubobjectcollection)), None)


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
    files = wikimigrate(pages, set(v['page_name'] for v in order))
    if files:
        yield {
            'name': 'ALL',
            'files': files,
            'author_name': WIKI_FIXUP_AUTHOR_NAME,
            'author_email': WIKI_FIXUP_AUTHOR_EMAIL,
            'message': WIKI_FIXUP_MESSAGE,
            'date': datetime.now().replace(microsecond=0),
        }


def wikimigrate(pages, page_names):
    """
    Convert the wiki page to the GitHub format
    """

    # To find old headers. Variants:
    #   .h1 Title  .h2 Title
    re_heading = re.compile(r'^h(\d). ', re.M)

    # To find [[links]]. Variants:
    #   [[Wiki]]  [[Wiki|Printed name]]  [[url:someurl]]  [[url:someurl|Printed name]]
    # 1=prefix:, 2=first group, 3=| second group, 4=second group
    re_link = re.compile(r'\[\[(\w+?:)?(.+?)(\|(.+?))?\]\]', re.M)

    files = {}
    for k, v in pages.items():
        if not v:
            continue
        contents = v

        # Replacing .h1 .h2 headers
        contents = re_heading.sub(lambda m: '#' * int(m[1]) + ' ', contents)

        # Replacing [[links]]
        def _link(m):
            """ Link formatter replace callback """
            if not m[1]:
                # Is a wiki link
                m2 = m[2]
                # Special fixups
                if m2 == 'tips/index':
                    m2 = 'Tips'
                if m2 == 'platforms/index':
                    m2 = 'Platforms'
                if m2 not in page_names:
                    logging.warning(f"Wiki links to unknown page '{m2}'")
                if not m[4]:
                    # Bare wiki link
                    return f"[[{m2}]]"
                # Wiki link with name
                return f"[[{m[4].strip()}|{m2}]]"
            if m[1] == 'url:':
                # Plain link
                if not m[4]:
                    return f"{m[2]}"
                # Link with name
                return f"[{m[4].strip()}]({m[2]})"
            # Fallthrough
            logging.warning(f"Unknown wiki link '{m[1]}'")
            return f"[[{m[1] or ''}{m[2] or ''}{m[3] or ''}]]"
        contents = re_link.sub(_link, contents)

        if contents == v:
            continue
        files[k] = contents

    return files


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

    subcmd = subparser.add_parser('userscrape', help="Scrape users from Assembla")
    subcmd.add_argument('dump', help="Output file to store users scrape")
    subcmd.set_defaults(func=cmd_userscrape)

    subcmd = subparser.add_parser('wikiconvert', help="Convert to GitHub wiki repo")
    subcmd.add_argument('repo', help='cloned git wiki repo directory')
    subcmd.add_argument('--dry-run', '-n', action="store_true", help="Do not commit any data")
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

    live = not runoptions.dry_run

    # Check arguments
    wikirepo = pathlib.Path(runoptions.repo)
    if not wikirepo.is_dir():
        parser.error(f"{str(wikirepo)}: Not a directory")

    # Open git repo
    repo = git.Repo(wikirepo)
    workdir = pathlib.Path(repo.working_tree_dir)

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
            fname = pathlib.Path(workdir, name)
            fname.write_bytes(contents.encode())
            files.append(str(fname))

        # Add the files
        repo.index.add(files)

        actor = git.Actor(commit['author_name'], commit['author_email'])
        date = commit['date'].astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        if live:
            repo.index.commit(
                commit['message'],
                author=actor,
                author_date=date,
                committer=actor,
                commit_date=date,
            )


# -----------------------------------------------------------------------------
#  Tickets conversion
def cmd_tickets(parser, runoptions, auth, data):

    # Check for required auth fields
    check_config(auth, parser, ('username', 'password'))

    # Parse the dump file data
    for milestone in data['milestones']:
        milestone['githubtitle'] = '[#{0}] - {1}'.format(milestone['id'], milestone['title'])
        milestone['assemblakey'] = '[#{0}]'.format(milestone['id'])
        ASSEMBLA_MILESTONES.append(milestone)

    for ticket in data['tickets']:
        ticket['githubtitle'] = '[#{0}] - {1}'.format(ticket['number'], ticket['summary'])
        ticket['assemblakey'] = '[#{0}]'.format(ticket['number'])
        ASSEMBLA_TICKETS.append(ticket)

    for ticketstatus in data['ticket_status']:
        ticketstatus['githubtitle'] = '[#{0}] - {1}'.format(ticketstatus['id'], ticketstatus['name'])
        ticketstatus['assemblakey'] = '[#{0}]'.format(ticketstatus['id'])
        ASSEMBLA_TICKET_STATUSES.append(ticketstatus)

    for ticketcomment in data['ticket_comments']:
        ticketcomment['assemblakey'] = '[#{0}]'.format(ticketcomment['id'])
        ticketcomment['createdate'] = datetime.fromisoformat(ticketcomment['created_on']).strftime('%Y-%m-%d %H:%M')
        ASSEMBLA_TICKET_COMMENTS.append(ticketcomment)

    # establish github connection
    ghub = github.Github(auth['username'], auth['password'])

    repo = ghub.get_repo(runoptions.repo)
    GITHUB_ISSUES = [x for x in repo.get_issues()]
    GITHUB_MILESTONES = [x for x in repo.get_milestones()]
    GITHUB_USERS = [x for x in repo.get_collaborators()]

    logging.info('Refreshing milestones->milestones...')
    for assemblamilestone in ASSEMBLA_MILESTONES:
        githubmilestone = findgithubobjectbyassemblaid(assemblamilestone['assemblakey'], GITHUB_MILESTONES)
        if not githubmilestone:
            logging.info('creating milestone: [{0}]'.format(assemblamilestone['githubtitle']))
            githubmilestone = repo.create_milestone(assemblamilestone['githubtitle'])
        else:
            logging.info('found existing milestone [{0}]'.format(assemblamilestone['githubtitle']))
        githubmilestone.edit(assemblamilestone['githubtitle'], description=assemblamilestone['description'])
    GITHUB_MILESTONES = repo.get_milestones()

    logging.info('Refreshing tickets->issues...')
    for assemblaticket in ASSEMBLA_TICKETS:
        assemblakey = assemblaticket['assemblakey']
        logging.info('Working on assembla ticket #{0}'.format(assemblakey))
        githubissue = findgithubobjectbyassemblaid(assemblakey, GITHUB_ISSUES)

        # create or find github issue using assembla key
        if not githubissue:
            logging.debug('Creating new issue: [{0}]'.format(assemblakey))
            githubissue = repo.create_issue(assemblaticket['githubtitle'], body=(assemblaticket['description'] or '(no description)'))
        else:
            logging.debug('Found existing issue: [{0}]'.format(assemblaticket['githubtitle']))

        logging.debug('Attempting to locate the milestone for assembla ticket #{0}'.format(assemblakey))
        assemblamilestone = next(iter(filter(lambda x: x['id'] == assemblaticket['milestone_id'], ASSEMBLA_MILESTONES)), None)

        # create or find github milestone using assembla key
        if assemblamilestone:
            logging.debug('Found assembla milestone for assembla ticket #{0}. Finding associated milestone.'.format(assemblakey))
            githubmilestone = findgithubobjectbyassemblaid(assemblamilestone['assemblakey'], GITHUB_MILESTONES) or github.GithubObject.NotSet

        logging.debug('Attempting to locate ticket status for assembla ticket #{0}'.format(assemblakey))
        assemblaticketstatus = next(iter(filter(lambda x: x['id'] == assemblaticket['ticket_status_id'], ASSEMBLA_TICKET_STATUSES)))
        githubissuestatus = ASSEMBLA_TICKET_STATUS_TO_GITHUB_ISSUE_STATUS.get(assemblaticketstatus['name'], 'open')

        logging.debug('Attempting to locate assigned user for assembla ticket #{0}'.format(assemblakey))
        githubuserid = ASSEMBLA_USERID_TO_GITHUB_USERID.get(assemblaticket['assigned_to_id'], None)
        githubuser = next(iter(filter(lambda x: x.login == githubuserid, GITHUB_USERS)), github.GithubObject.NotSet)

        logging.debug('Updating github issue for ticket #{0}'.format(assemblakey))
        assemblaticket['description'] = assemblaticket['description'] or '(no description)'
        githubissue.edit(assemblaticket['githubtitle'], body=assemblaticket['description'], milestone=githubmilestone, state=githubissuestatus, assignee=githubuser)

        # assembla ticket comments -> github issue comments
        logging.debug('Rebuilding issue comments for issue #{0}'.format(assemblaticket['assemblakey']))
        assemblaticketcomments = filter(lambda x: x['ticket_id'] == assemblaticket['id'], ASSEMBLA_TICKET_COMMENTS)

        # wipe out all the github issue comments and rebuild every time.
        # probably a better way but the github api has limited support for comment modification.
        for githubissuecomment in githubissue.get_comments():
            githubissuecomment.delete()
        for assemblaticketcomment in assemblaticketcomments:
            if assemblaticketcomment['comment']:
                githubissue.create_comment('({}) - {}'.format(assemblaticketcomment['createdate'], assemblaticketcomment['comment']))


if __name__ == "__main__":
    main()
