# Parquet to NDJSON (newline delimited JSON) converter

Simple CLI app for converting parquet to newline delimited JSON. Built with [Polars](https://docs.pola.rs/) and [Typer](https://typer.tiangolo.com/).

## Instructions with docker or uv
### docker
- Install docker
- `docker build -t p2j .`
- `docker run -v /path/to/files:/mnt -it p2j /mnt/<PARQUET> /mnt/<JSON>`
Or if reading parquet from Google bucket:
- `docker run -v /path/to/files:/mnt -v /path/to/gcp/credentials.json:/app/credentials.json -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json -it p2j gs://<PATH_TO_PARQUET> /mnt/<JSON>`


### uv
- Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Install to virtual env `uv venv; source .venv/bin/activate; uv pip install "git+https://github.com/opentargets/parquet2json"` 
- `parquet2json <PARQUET_IN> <JSON_OUT>`

### Help
```
 Usage: parquet2json [OPTIONS] PARQUET JSON                          
                                                             
╭─ Arguments ───────────────────────────────────────────────────────────╮
│ *    parquet      TEXT    Input path/URI to parquet. [default: None]  │
│                           [required]                                  │
│      json         [JSON]  Output JSON path, or leave empty for STDOUT │
│                           [default: None]                             │
╰───────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────╮
│ --help  -h        Show this message and exit.                         │
╰───────────────────────────────────────────────────────────────────────╯
```

## Copyright
Copyright 2014-2024 EMBL - European Bioinformatics Institute, Genentech, GSK, MSD, Pfizer, Sanofi and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please
see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.