import subprocess
import os
import datetime
import sys

# Use Python interpreter from the current virtual environment
PYTHON_EXECUTABLE = sys.executable

# Define core directories
ROOT_DIR = "."
SCRIPTS_DIR = os.path.join(ROOT_DIR, "scripts")
LOG_FILE = os.path.join(ROOT_DIR, "documentation", "workflow_log.py")

# Try to read a custom output directory from documentation/output_dir.txt
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
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {message}"
    print(full_msg)

    # Ensure the directory for the log file exists
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    with open(LOG_FILE, "a") as f:
        f.write(full_msg + "\n")


def run_script(script_path, output_dir):
    """
    Executes a Python script as a subprocess, passing the output directory
    as a command-line argument.

    Args:
        script_path (str): Path to the Python script to run.
        output_dir (str): Directory to pass to the script via --output-dir.
    """
    try:
        result = subprocess.run(
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

def main():
    """
    Main workflow function. Finds all eligible Python scripts in the scripts directory
    and runs them sequentially, logging the outcome of each.
    """
    log("Starting workflow...")

    # Verify scripts directory exists
    if not os.path.exists(SCRIPTS_DIR):
        log(f"Scripts directory not found: {SCRIPTS_DIR}")
        return

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # List all .py files that don't start with "_" for execution
    py_files = sorted(
        f for f in os.listdir(SCRIPTS_DIR)
        if f.endswith(".py") and not f.startswith("_")
    )

    if not py_files:
        log("No Python scripts found in scripts directory.")
        return

    # Run each script and log the outcome
    for py_file in py_files:
        full_path = os.path.join(SCRIPTS_DIR, py_file)
        log(f"Running: {py_file}")
        run_script(full_path, OUTPUT_DIR)

    log("Workflow complete.")

if __name__ == "__main__":
    main()
