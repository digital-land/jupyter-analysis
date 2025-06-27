import subprocess
import os
import datetime
import sys
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from airflow.models import Variable, Connection
from airflow.hooks.base import BaseHook

def get_sharepoint_credentials():
    """
    Fetch SharePoint username and password from Airflow connection 'sharepoint_conn'
    """
    conn = BaseHook.get_connection("sharepoint_conn")
    username = conn.extra_dejson.get("username")
    password = conn.extra_dejson.get("password")
    return username, password

# Paths
PYTHON_EXECUTABLE = sys.executable
ROOT_DIR = "/opt/airflow/monitoring_data_collection_tool"
SCRIPTS_DIR = os.path.join(ROOT_DIR, "scripts")
LOG_FILE = os.path.join(ROOT_DIR, "documentation/logs", "workflow_log.txt")

# Output directory setup
DOC_OUTPUT_PATH = os.path.join(ROOT_DIR, "documentation", "output_dir.txt")
DEFAULT_OUTPUT_DIR = os.path.join(ROOT_DIR, "outputs")

try:
    if os.path.isfile(DOC_OUTPUT_PATH):
        with open(DOC_OUTPUT_PATH, "r") as f:
            custom_output_dir = f.read().strip()
            OUTPUT_DIR = custom_output_dir if custom_output_dir else DEFAULT_OUTPUT_DIR
    else:
        OUTPUT_DIR = DEFAULT_OUTPUT_DIR
except Exception as e:
    print(f"Warning: Failed to read output_dir.txt. Using default. Error: {e}")
    OUTPUT_DIR = DEFAULT_OUTPUT_DIR


def log(message):
    """
    Logs a timestamped message to both the console and a log file.

    This function prints the message to standard output and appends it to the
    log file defined by the global LOG_FILE path. If the log file directory
    does not exist, it is created automatically.

    Parameters:
        message (str): The message to be logged.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {message}"
    print(full_msg)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")

def run_script(script_path, output_dir):
    """
    Executes a Python script with an output directory argument and logs the result.

    This function runs an external Python script as a subprocess, passing the
    specified output directory as a command-line argument. It captures and logs
    the result of the execution, including any errors.

    Parameters:
        script_path (str): The path to the Python script to run.
        output_dir (str): The directory path to pass as the '--output-dir' argument.
    """
    try:
        subprocess.run(
            [PYTHON_EXECUTABLE, script_path, "--output-dir", output_dir],
            capture_output=True,
            text=True,
            check=True
        )
        log(f"SUCCESS: {script_path}")
    except subprocess.CalledProcessError as e:
        log(f"FAIL: {script_path}")
        log(f"Error:\n{e.stderr.strip()}")
    except Exception as e:
        log(f"ERROR: {script_path} - {str(e)}")

def ensure_folder(parts, root_folder, ctx):
    """
    Ensures a nested folder structure exists in SharePoint, creating folders as needed.

    Traverses through the given list of folder names (`parts`) starting from `root_folder`.
    For each folder in the path, it attempts to create it. If the folder already exists,
    it loads the existing folder instead. Returns the deepest folder in the path.

    Parameters:
        parts (list of str): A list of folder names representing the nested folder path.
        root_folder (office365.sharepoint.folders.folder.Folder): The starting root folder object.
        ctx (ClientContext): The SharePoint client context for executing operations.

    Returns:
        office365.sharepoint.folders.folder.Folder: The final subfolder object in the nested structure.
    """
    current_folder = root_folder
    for part in parts:
        try:
            sub_folder = current_folder.folders.add(part)
            ctx.execute_query()
        except Exception:
            # Folder may already exist; try to get it instead
            sub_folder = current_folder.folders.get_by_url(part)
            ctx.load(sub_folder)
            ctx.execute_query()
        current_folder = sub_folder
    return current_folder

def upload_to_sharepoint(ctx, folder, local_file_path):
    """
    Uploads a local file to a specified SharePoint folder.

    Reads the contents of a local file and uploads it to the given SharePoint
    folder using the provided client context. Logs the server-relative URL of the
    uploaded file upon success.

    Parameters:
        ctx (ClientContext): The SharePoint client context for executing the upload.
        folder (office365.sharepoint.folders.folder.Folder): The SharePoint folder to upload the file to.
        local_file_path (str): The full local path of the file to be uploaded.
    """
    file_name = os.path.basename(local_file_path)
    with open(local_file_path, "rb") as f:
        content = f.read()
    target_file = folder.upload_file(file_name, content).execute_query()
    log(f"Uploaded to SharePoint: {target_file.serverRelativeUrl}")

def get_existing_folder(ctx, base_url, parts):
    """
    Traverses a nested folder path in SharePoint and returns the final folder.

    Starting from a base URL, this function iteratively appends each folder name
    in `parts` to the path and attempts to load the corresponding folder from SharePoint.
    If any folder in the path does not exist or fails to load, an exception is raised.

    Parameters:
        ctx (ClientContext): The SharePoint client context for executing requests.
        base_url (str): The server-relative URL to the base SharePoint folder.
        parts (list of str): A list of folder names representing the nested path to traverse.

    Returns:
        office365.sharepoint.folders.folder.Folder: The final folder object in the path.
    
    Raises:
        Exception: If any folder in the path cannot be accessed.
    """
    current_path = base_url
    for part in parts:
        current_path = f"{current_path}/{part}"
        folder = ctx.web.get_folder_by_server_relative_url(current_path)
        ctx.load(folder)
        try:
            ctx.execute_query()
        except Exception as e:
            raise Exception(f"Failed to access folder: {current_path}. Error: {e}")
    return folder

def upload_all_outputs_to_sharepoint(output_dir):
    """
    Uploads all CSV files from the specified output directory to SharePoint.

    This function performs the following actions:
    1. Authenticates with SharePoint using credentials from a local file.
    2. Uploads all `.csv` files from `output_dir` to the base SharePoint folder:
       `/20. Data Management/Reporting/Data files`.
    3. Creates a dated archive folder inside `.../Data files/old files/` using the current date.
    4. Uploads a copy of each `.csv` file to this dated archive folder for record-keeping.

    If the "old files" or dated archive folders already exist, they are reused.

    Parameters:
        output_dir (str): Local directory path containing the CSV files to upload.

    Raises:
        Exception: If the base SharePoint folder cannot be accessed.
    """
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    parent_url = "/sites/DigitalPlanning/Shared Documents/20. Data Management/Reporting/Data files"
    old_files_folder_name = "old files"

    username, password = get_sharepoint_credentials()


    site_url = "https://mhclg.sharepoint.com/sites/DigitalPlanning"
    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

    # Load base folder (parent)
    try:
        base_folder = ctx.web.get_folder_by_server_relative_url(parent_url)
        ctx.load(base_folder)
        ctx.execute_query()
    except Exception as e:
        raise Exception(f"Cannot access base folder.\nURL: {parent_url}\nError: {e}")

    # Ensure "old files" folder exists
    old_files_url = f"{parent_url}/{old_files_folder_name}"
    try:
        ctx.web.folders.add(old_files_url).execute_query()
        log(f"Created 'old files' folder: {old_files_url}")
    except Exception:
        log(f"'old files' folder already exists.")

    # Create today's dated folder inside "old files"
    dated_folder_url = f"{old_files_url}/{today_str}"
    try:
        ctx.web.folders.add(dated_folder_url).execute_query()
        log(f"Created dated archive folder: {dated_folder_url}")
    except Exception:
        log(f"Dated archive folder already exists: {dated_folder_url}")

    # Load both folders
    base_upload_folder = ctx.web.get_folder_by_server_relative_url(parent_url)
    archive_folder = ctx.web.get_folder_by_server_relative_url(dated_folder_url)
    ctx.load(base_upload_folder)
    ctx.load(archive_folder)
    ctx.execute_query()

    # Upload each CSV to both base and archive folders
    for file in os.listdir(output_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(output_dir, file)
            log(f"Uploading '{file}' to base folder and archive folder...")
            try:
                with open(file_path, "rb") as f:
                    content = f.read()
                base_upload_folder.upload_file(file, content).execute_query()
                archive_folder.upload_file(file, content).execute_query()
                log(f"Uploaded: {file}")
            except Exception as e:
                log(f"Failed to upload '{file}': {e}")

    log(f"All files uploaded to base and archived in 'old files/{today_str}'.")

def main():
    """
    Executes the full data processing workflow.

    This function performs the following steps:
    1. Logs the start of the workflow.
    2. Verifies the existence of the `scripts` directory.
    3. Creates the output directory if it does not exist.
    4. Identifies all Python scripts (excluding those starting with "_") in the `scripts` directory.
    5. Executes each script using `run_script()`, passing the output directory as an argument.
    6. Once all scripts are executed, uploads all generated CSV files to SharePoint using
       `upload_all_outputs_to_sharepoint()`.
    7. Logs completion status and any errors during script execution or upload.

    This function is intended to be the main entry point of the workflow.
    """
    log("Starting workflow...")

    if not os.path.exists(SCRIPTS_DIR):
        log(f"Scripts directory not found: {SCRIPTS_DIR}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    py_files = sorted(
        f for f in os.listdir(SCRIPTS_DIR)
        if f.endswith(".py") and not f.startswith("_")
    )

    if not py_files:
        log("No Python scripts found in scripts directory.")
        return

    for py_file in py_files:
        full_path = os.path.join(SCRIPTS_DIR, py_file)
        log(f"Running: {py_file}")
        run_script(full_path, OUTPUT_DIR)

    log("All scripts complete. Uploading to SharePoint...")
    upload_all_outputs_to_sharepoint(OUTPUT_DIR)
    log("Workflow complete.")


if __name__ == "__main__":
    main()
