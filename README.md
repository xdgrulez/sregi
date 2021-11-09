# sregi

`sregi` is a Python-based tool built for Schema Registry (SR) migration also suitable for Confluent Cloud.

## Installation

`sregi` has the following requirements (see also "requirements.txt"):
* requests (2.23.0)
* pyyaml (5.3.1)

## Usage

### YAML files

To connect to a Schema Registry (SR), `sregi` makes use of YAML files specifying its URL, user and password. Here is an example YAML file `foobar.yaml`:
```
url: https://foo.bar
user: foo
password: bar
```
`sregi` expects these YAML file to be stored in the `srs` sub-directory.

### Commands

sregi can execute three commands:
1. download
2. upload
3. deleteall

#### Download

To download the SR denoted by `foobar.yaml`:
```
python sregi.py download foobar
```
This will download the entire SR including all subjects, subject versions, the global compatibility configurations, and the subject-specific compatibility configurations to disk into the sub-directory `srs/foobar`.

The directory structure will look as follows:
```
foobar
  global_config.json
  subject1-value
    1.json
	2.json
  subject2-value
    config.json
    1.json
```

Here, `global_config.json` contains the global compatibility configuration. `subject1-value` and `subject2-value` are subjects. Subject `subject1-value` has two subject versions `1` and `2`, which are stored in the two JSON files `1.json` and `2.json`. Subject `subject2-value` has one subject version `1`, which is stored in the JSON file `1.json`. In addition the subject has a subject-specific compatibility configuration stored in `config.json`.

#### Upload

To migrate the source SR denoted by `foobar.yaml` to a target SR, you create a new YAML file for the target SR, e.g. called `target.yaml`:
```
url: https://tar.get
user: tar
password: get
```

Then you copy the sub-directory `foobar` to another sub-directory `target` (or you just rename it if you are not interested in keeping it).

You may now clean up the subjects in the `target` sub-directory if you do not need all the subjects/subject versions in the `target` SR. You might even add/update/delete global or subject-specific compatibility configurations.

Next, you upload the `target` sub-directory to the target SR as follows:
```
python sregi.py upload target
```
This will upload the contents of the `target` sub-directory to the SR denoted by `target.yaml` using a special SR mode called `IMPORT`.

**NB**: Please be careful - using this command is dangerous - `sregi` does not do any syntax checks - it just uploads the entire directory... Only use it after thoroughly checking that the content of the directory to be uploaded is ok, and the referenced YAML file points to exactly the SR that you would like to upload to.

#### Deleteall

You can only use the upload command on empty SRs, i.e. SRs containing no subjects. The reason for this is technical - `sregi upload` makes use of the SR `IMPORT` mode and you can only switch an SR to `IMPORT` mode when its clear of any subjects.

To delete all subjects from a SR, you can use the `deleteall` command:
```
python sregi.py deleteall target
```
**NB**: Please be careful - using this command is dangerous for obvious reasons. Only use it after thoroughly checking that the referenced YAML file points to exactly the SR that you would like to delete.
