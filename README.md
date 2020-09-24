# assembla2github
## migrate assembla project -> github

> This tool was forked from https://github.com/PeteW/assembla2github and this
repo represents the tooling made for converting the PortAudio project from
Assembla to GitHub.

The tool has been significantly expanded since the original script by
@PeteW.

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

3. in assembla source site go to settings -> export and import -> submit a request to export your
   tickets. the request is queued and may take several minutes. You'll eventually gain access to a
   downloadable backup of assembla tickets in a JSON-like format named `dump.js`.

4. edit the top of the assembla2github.py file, there are several project-specific mappings you must
   customize such as user and status mappings

5. create `auth.json` from `auth-template.json`. Edit file and set the passwords and IDs.
   The `assembla_*` fields are only required if running `userscrape` or `wikiscrape`.

6. create `config.json` from `config-template.json` and fill in the fields. The `dumpfile` file
   must point to your downloaded `dump.js` file.

### usage

Run the python script:
```
venv/bin/python assembla2github.py [--config=myconfig.json] [options...] COMMAND [arguments...]
```

(Windows use `venv\Scripts\python assembla2github.py ...`)

The tool operates on the Assembla dumpfile specified in the `dumpfile` setting in the config file specified by `--config`.
If no `--config` option is specified, it will try to read `config.json` in the current directory.
The tool provides multiple operations on the Assembla dataset. The following `COMMAND` specifies
the wanted operation.

 * **`userscrape`** - Use the Assembla API to fetch the user names and store in a user
        dump file. This file can be used later with the `userdump` config option to merge with the
        main data.
 * **`wikiscrape`** - Use the Assembla API to fetch the wiki page contents and store in
        a dump file. This file can be used later with the `wikidump` config option to merge with the
        main data.
 * **`wikiconvert`** - Extract the wiki data from the dump file database and commit all
        the changes into the git repo that can be pushed to GitHub. It converts the Assembla
        Wiki markup to GitHub markdown.
 * **`ticketsconvert`** - FIXME.

Helper commands for debug and inspection:

 * **`dump`** - Debug tool to dump the Assembla dataset
 * **`lsusers`** - List all users found in dump file.
 * **`lswiki`** - List all wiki pages found in dump file.

The tool supports `--help`. Specifying no `COMMAND` will show all available global options. Specifying
`--help` after a `COMMAND` will show the options for that command.


### procedure

The full procedure for converting the Assembla project to GitHub is outlined below.

> **NOTE:** Please pay heed to the `WARNING` messages the tools might output when commands
> are run. It could indicate missing or incorrect data during conversion.


#### (A) Assembla data export

1. Get the latest Assembla `dump.js` file from the Assembla site (step 3 under installation/setup
   above).

   Edit `config.json` to point the `dumpfile` field to the downloaded `dump.js` file.

2. Fetch the user data from Assembla using:

        assembla2github.py userscrape users.js

   Edit `config.json` and add this file to the `userdump` field.

3. Fetch the wiki pages from Assembla API:

        assembla2github.py wikiscrape wikipages.js

   Edit `config.json` and add this file to the `wikidump` field.

> **Testing / validation**: Use the following command to list the info for all
> users after the user scrape.
>
>       assembla2github.py lsusers


#### (B) Create GitHub repo

1. Create the GitHub repo.

   Edit `config.json` and set the `repo` field to value `<user>/<repo>`.

2. Import or push the git repo to GitHub


#### (C) Import Wiki

> **Testing / validation**: Use `assembla2github.py lswiki` to view and test wiki
> conversion.
>
> * `lswiki -q` - View all conversion warnings without listing the wiki pages
> * `lswiki -B before -A after` - Dump all wiki texts into files `before` and `after`
>                                 files before and after github conversion. Useful for external
>                                 comparison of the markdown conversion using diff

1. Open the GitHub Wiki page for the repo in a web browser and create dummy first
   page. Contents and title does not matter.

2. Convert the wiki data

        assembla2github.py wikiconvert wikirepo

3. The `wikirepo` directory contains the Wiki git repo. Enter the directory, inspect it and push it
   to GitHub

        cd wikirepo
        git push


#### (D) Import tickets

> **Testing / validation**: Use `assembla2github.py lstickets` to test and view list tickets and test
> github conversion prior to production conversion. Useful variants:
>
> * `lstickets -q` - View all conversion warnings without listing the tickets
> * `lstickets` - View all Assembla tickets and ticket events. Options `-d` and `-c` will dump description
>                 and comment texts inline.
> * `lstickets -g` - View all Github issues after conversion
> * `lsticket 154` - To view specific ticket(s) add them to the command line
> * `lstickets -B before -A after` - Save all description and comment texts into files `before` and `after`
>                                    files before and after github conversion. Useful for external
>                                    comparison with diff

1. Convert the tickets

        assembla2github.py ticketsconvert
