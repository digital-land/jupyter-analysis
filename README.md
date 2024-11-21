# Digital Jupyter Analysis
This repository contains repeatable analysis performed against the data contained within the digital land platform. 

Jupyter will allow us to create repeatable analysis and also generate a record of analysis done before to help share lessons learned.

## Repo Structure

The repo is organised as below, with content split into three top-level directories:

```
|- Analysis
|   |- [YYYY-MM]_[analysis_name]
|       |- [analysis_name].ipynb
|    ...
|- Reports
|   |- 0_archive
|   |- Report_one
|       |- Report_one.ipynb
|   ...
|- Tools
|   |- 0_archive
|   |- Tool_one
|       |- Tool_one.ipynb
|    ...
```

**Analysis**: One off pieces of work, usually focussed on a specific problem like a particular data quality investigation, or testing an approach for a report. Not guaranteed to be re-runnable for the long term but useful to save as a record.

**Reports**: A re-runnable notebook which should be well tested. Expect it to produce some standard outputs like charts or tables. These might be useful summaries for stakeholders, or data which needs some further interpretation but is useful for monitoring or investigations.

**Tools**: A very task-focussed notebook to do one, clearly defined thing. Might be a longer thing or just a short snippet.

Each notebook should be stored in its directory along with any helper scripts. We should avoid editing other's analysis except to update when it's no longer working.

Analysis notebooks should have the year and month in the name of their parent directory, as this makes it easier to identify what were often one-off pieces of work. Notebooks in the Reports and Tools directories are more likely to be maintained, so instead those two directories each contain a `0_archive` directory. Once a report or tool is no longer used it should be moved here to help keep the active list tidy.


## Best practice
### Data
This repository SHOULD NOT be used to store data. Data should be downloaded from our service when notebooks run. The data directory is ignored by git and can be used as local storage to sync data to. Notebooks should save outputs to the data directory to avoid committing data to this repo.

BE WARNED that many files we work with can be large so be careful when running notebooks as they may contain commands to download multi-GB sqlite databases from Datasette.

### Naming
Wherever possible give your report or tool an action-orientated name which uses simple language and makes clear what it does. In practice this means the name will often start with a verb, like "find" or "check".

> ❌ `CA_duplicate_report.ipynb`    
> 
> ✅ `Find_conservation_area_duplicates.ipynb`

This isn't so important for analysis notebooks, which can often have not so clear a purpose, but still try and be descriptive!

## Dependencies
We aim to set up the same environment as in the pipline, hence primarily installing digital-land-python but there may be additional tools for data manipulation or visualisation that can be added to the requirements. You can also change requirements files to download specific branches of digital-land-python to test new features before merging.
