# Data Portal Archiver

The goal of this project is to preserve the data (and its associated metadata) from open data portals. For this, the data is uploaded with its metadata to the Internet Archive and the portal metadata is saved in a gh repo. 



## Getting Started

Can be installed directy from PyPI:

```bash
pip install data-portal-archiver
```

You need to create the configuration file and add the portals to download:

```bash
wget link-config-example  # TODO
mv portals_example.toml portals.toml
```

TODO: show portals.toml

Also you need an account on the Internet archive and get the api keys from [here](https://archive.org/account/s3.php).

And finally we can run it:
```bash
export IA_ACCESS_KEY=some_super_secret
export IA_SECRET_KEY=another_super_secret

dpa section_name
```

You can also pass the SECTION_NAME as an environment variable:

```
export IA_ACCESS_KEY=some_super_secret
export IA_SECRET_KEY=another_super_secret
export SECTION_NAME=example_section

dpa 
```

## Running as a github-action

TODO

## Dev

TODO