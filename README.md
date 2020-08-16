# assembla2github
## migrate assembla project -> github

### features/function
migrates a full assembla site's tickets/milestones to github's issue format
* Ticket status -> issue status (open/closed)
* Ticket comment/conversation history
* Ticket user assignment
* Original assembla ticket number preserved
* Ticket/milestone associations

**repeatable** - can execute multiple times. The generated issues/milestone names are prefixed with the original assembla ID numbers. Pre-existing issues/milestones having names which start with assembla identifiers will be updated with new information and not duplicated

### installation/setup

1. Install Python 3.7
2. Install a Python virtual environment and install the packages

        python -mvenv venv
        venv/bin/python -mpip install --upgrade pip
        venv/bin/pip install -r requirements.txt

   Procedure for Windows:

        py -3.7 -mvenv venv
        venv\Scripts\python -mpip install --upgrade pip
        venv\Scripts\pip install -r requirements.txt

3. in assembla source site go to settings -> export and import -> submit a request to export your tickets. the request is queued and may take several minutes. You'll eventually gain access to a downloadable backup of assembla tickets in a JSON-like format.
4. edit the top of the assembla2github.py file, there are several project-specific mappings you must customize such as user and status mappings
5. create a `auth.json` and set the passwords and IDs. Rename `auth-template.json` into `auth.json` and
fill in the fields. The fields with "assembla_" prefix is only required if `userscrape` or `wikiscrape`
is run.

### usage

Run the python script:
```
venv/bin/python assembla2github.py --dumpfile=/path/to/dump.js [options...] COMMAND [arguments...]
```

(Windows use `venv\Scripts\python assembla2github.py ...`)

The tool operates on the Assembla dumpfile specified by `--dumpfile` option. Additional data
will be read and merged with the Assembla dumpfile database using the extra options `--wikidump` and
`--userdump`. The Wiki dump can be created using the `wikiscrape` command and the user dump with
`userscrape`.

`COMMAND` specifies the wanted operation. Current list of commands:

 * **`users`** - List all users found in dump file.
 * **`userscrape`** `FILE` - Use the Assembla API to fetch the user names and store in a user
        dump file. This file can be used later with the `--userdump` option to merge with the main
        data.
 * **`wiki`** - List all wiki pages found in dump file.
 * **`wikiscrape`** `FILE` - Use the Assembla API to fetch the wiki page contents and store in
        a dump file. This file can be used later with the `--wikidump` option to merge with the
        main data.
 * **`wikiconvert`** `REPO` - Extract the wiki data from the dump file database and commit all
        the changes into the git repo `REPO`.

The tool supports `--help`. Specifying no `COMMAND` will show all available global options. Specifying
`--help` after a `COMMAND` will show the options for that command.


### procedure

The procedure for converting the Assembla project to GitHub is:

1. Get the latest Assembla `dump.js` file.
2. Fetch the list of users from Assembla API:

        assembla2github.py --dump dump.js userscrape users.js

3. Fetch the wiki pages from Assembla API:

        assembla2github.py --dump dump.js wikiscrape wikipages.js

4. Create a new GitHub repo.

4. *(WIKI)* Open Wiki on the GitHub repo and create dummy first page. Contents does not matter.
   Clone repo with

        git clone git@github.com:<USER>/<REPO>.wiki.git wiki

5. *(WIKI)* Convert the wiki data

        assembla2github.py --dump dump.js --userdump users.js --wikidump wikipages.js \
                wikiconvert wiki

6. *(WIKI)* Push the repo to GitHub

        cd wiki
        push push

To be continued...
