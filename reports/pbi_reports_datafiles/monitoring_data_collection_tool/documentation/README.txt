Monitoring Data Collection Tool
===============================

This project automates the retrieval, analysis, and SharePoint upload of monitoring datasets from the Open Digital Planning (ODP) Datasette service.

------------------------------------------------------------
1. SETUP INSTRUCTIONS
------------------------------------------------------------

1.1. Create the Virtual Environment
-----------------------------------
Use the provided batch file to set up your environment and install dependencies:

    setup_env.bat

This will:
- Create a `.venv` Python virtual environment in the project directory
- Activate the environment
- Install all required libraries from `documentation\utils\requirements.txt`

------------------------------------------------------------
2. RUNNING THE TOOL
------------------------------------------------------------

To run the full workflow:

    python run.py

This script:
- Executes all scripts in the `scripts\` folder
- Stores results in the `outputs\` folder
- Uploads files to SharePoint (if credentials provided)

To override the default output directory, edit:

    documentation\output_dir.txt

------------------------------------------------------------
3. FOLDER STRUCTURE
------------------------------------------------------------

monitoring_data_collection_tool\
│
├── run.py                          # Main runner script
├── setup_env.bat                  # Environment setup script
├── sharepoint_credentials.txt     # SharePoint credentials
│
├── scripts\                       # All scripts executed by run.py
│   ├── duplicate_geometry_expectations.py
│   ├── endpoint_dataset_issue_type_summary.py
│   ├── endpoints_missing_doc_urls.py
│   ├── other scripts # This folder is dynamic and new scripts can be added
├── documentation\
│   ├── logs\
│   │   └── workflow_log.txt
│   ├── scripts_documentation\
│   │   └── *.pdf
│   ├── utils\
│   │   ├── requirements.txt
│   │   └── specification.csv
│   └── output_dir.txt
│
└── outputs\                       # Created after scripts are run

------------------------------------------------------------
4. EXAMPLE: RUN A SINGLE SCRIPT
------------------------------------------------------------

    python scripts\operational_issues.py --output-dir outputs

------------------------------------------------------------
5. SHAREPOINT INTEGRATION
------------------------------------------------------------

To enable SharePoint upload, make sure you’ve filled in:
    sharepoint_credentials.txt

This is used by `run.py` to upload files after processing.

To disable this, comment or remove the `upload_all_outputs_to_sharepoint()` call in run.py.

------------------------------------------------------------
6. HELP
------------------------------------------------------------

For further support, contact the data engineering team or check the PDFs in:
    documentation\scripts_documentation\
