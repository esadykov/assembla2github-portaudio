# assembla2github
## migrate assembla tickets -> github issues

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

3. in assembla source site go to settings -> export and import -> submit a request to export your tickets. the request is queued and may take several minutes. You'll eventually gain access to a downloadable backup of assembla tickets in a JSON-like format.
4. edit the top of the assembla2github.py file, there are several project-specific mappings you must customize such as user and status mappings
5. create a `auth.json` and set the passwords and IDs. Use the `auth-template.json`
as template.

### running

Run the python script:
```
venv/bin/python assembla2github.py --dumpfile=/path/to/dump.js --auth auth.json COMMAND [options]
```

Where command is the wanted operation. Use `--help` to get information about the
options for the commands:

```
venv/bin/python assembla2github.py --help
venv/bin/python assembla2github.py wikiconvert --help
```
