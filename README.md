# researcher-searcher-setup
Methods to create starting data for researcher searcher

### Setup

Create `.env` file with location of config directory

```
CONFIG_PATH=config
```

Create `data.yaml` file in config directory

```
emails: path/to/email_file
orcid_data: path/to/orcid_file
pure_org_url: path/to_org_file
```

### todo

Fix issues with missing pubmed year when using eutils
- use year from orcid

Create instance for turing institute biographies?

