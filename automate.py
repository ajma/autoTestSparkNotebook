"""Automate VS Code Jupyter Notebook creation with screen recording."""

import argparse
import ctypes
from datetime import datetime
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time

import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from gspread.utils import InsertDataOption
import pyautogui
import pyperclip
from playwright.sync_api import sync_playwright

NOTEBOOK_SAVE_DIR = os.path.join(os.path.expanduser("~"), "spark_test_notebooks")
DEBUG_PORT = 9222

APP_CONFIGS = {
    "vscode": {
        "command": "code",
        "process_name": "Code.exe",
        "window_titles": ["Visual Studio Code", "Code"],
        "label": "VS Code",
    },
    "antigravity": {
        "command": "antigravity",
        "process_name": "Antigravity.exe",
        "window_titles": ["Antigravity"],
        "label": "Antigravity",
    },
}

PYSPARK_CODE = '''from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("PySparkSampleExample").getOrCreate()

# Create a sample DataFrame
data = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"), (6, "F"), (7, "G"), (8, "H"), (9, "I"), (10, "J")]
columns = ["id", "value"]
df = spark.createDataFrame(data, columns)

# Sample the DataFrame (approx. 50% of rows)
# withReplacement=False (default), fraction=0.5
sampled_df = df.sample(False, 0.5)

print("Original DataFrame count:", df.count())
print("Sampled DataFrame count:", sampled_df.count())
print("Sampled rows:")
sampled_df.show()
print("EXECUTION_COMPLETE_MARKER")'''


def validate_dependencies(app_config):
    missing = []
    if not shutil.which("ffmpeg"):
        missing.append("ffmpeg (install via: winget install Gyan.FFmpeg)")
    cmd = app_config["command"]
    label = app_config["label"]
    if not shutil.which(cmd):
        missing.append(f"{cmd} ({label} CLI - ensure {label} is installed and on PATH)")
    if missing:
        print("Missing dependencies:")
        for dep in missing:
            print(f"  - {dep}")
        sys.exit(1)


def start_recording(output_path):
    print(f"Starting screen recording -> {output_path}")
    proc = subprocess.Popen(
        [
            "ffmpeg", "-y",
            "-f", "gdigrab",
            "-framerate", "30",
            "-i", "desktop",
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            output_path,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)
    return proc


def stop_recording(proc):
    print("Stopping screen recording...")
    try:
        proc.stdin.write(b"q")
        proc.stdin.flush()
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print("Warning: ffmpeg did not stop gracefully, killing process.")
        proc.kill()
    except Exception as e:
        print(f"Warning: error stopping recording: {e}")
        proc.kill()


def check_notebook_output(notebook_path):
    """Read the saved .ipynb file and check cell outputs for the completion marker.

    Returns True if marker found (success), False if an error output is found,
    or None if execution hasn't finished yet.
    """
    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            nb = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError, OSError):
        return None

    for cell in nb.get("cells", []):
        for output in cell.get("outputs", []):
            if output.get("output_type") == "error":
                return False
            text_parts = output.get("text", [])
            if isinstance(text_parts, str):
                text_parts = [text_parts]
            text = "".join(text_parts)
            if "EXECUTION_COMPLETE_MARKER" in text:
                return True
    return None


def run_command_palette(page, command, timeout=10000):
    """Open the VS Code quick open (Ctrl+P), prefix with '>' to run a command, and select the first match."""
    page.keyboard.press("Control+P")
    input_box = page.locator(".quick-input-widget .input")
    input_box.wait_for(state="visible", timeout=5000)
    input_box.fill("")
    input_box.type(">" + command, delay=30)
    row = page.locator(".quick-input-list .monaco-list-row").first
    row.wait_for(state="visible", timeout=timeout)
    time.sleep(0.3)
    row.click()


def select_from_quick_pick(page, text, timeout=10000):
    """Type into an already-open quick pick and select the first match."""
    input_box = page.locator(".quick-input-widget .input")
    input_box.wait_for(state="visible", timeout=5000)
    input_box.fill("")
    input_box.type(text, delay=30)
    row = page.locator(".quick-input-list .monaco-list-row").first
    row.wait_for(state="visible", timeout=timeout)
    time.sleep(0.3)
    row.click()


def capture_jupyter_server_log(page, log_path):
    """Open the Output panel, select the Jupyter Server log channel, and save its contents."""
    # Use command palette to open the Jupyter Server output channel directly
    run_command_palette(page, "Output: Show Output Channels...")
    time.sleep(0.5)
    select_from_quick_pick(page, "Jupyter Server")
    time.sleep(1)

    # Select all text in the output panel and copy it
    page.keyboard.press("Control+A")
    time.sleep(0.3)
    page.keyboard.press("Control+C")
    time.sleep(0.5)

    log_text = pyperclip.paste()
    if log_text and log_text.strip():
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log_text)
        print(f"  Jupyter Server log saved to {log_path}")
    else:
        print("  Warning: Jupyter Server log was empty.")


def extract_cell_execution_time(page):
    """Extract the cell execution duration shown in the VS Code notebook UI.

    VS Code displays execution time (e.g. '14.2s') near the cell after it
    finishes running.  Returns the time in seconds, or None if not found.
    """
    # Try common selectors for the execution duration element
    selectors = [
        ".notebook-cell-execution-duration",
        ".cell-execution-duration",
        ".cell-status-item",
    ]
    for selector in selectors:
        elements = page.locator(selector).all()
        for el in elements:
            try:
                text = el.inner_text(timeout=1000).strip()
                # Match patterns like "14.2s", "2m 3s", "1m 30.5s", "500ms"
                m = re.match(r"(?:(\d+)m\s*)?(\d+(?:\.\d+)?)\s*s", text)
                if m:
                    minutes = int(m.group(1)) if m.group(1) else 0
                    seconds = float(m.group(2))
                    return minutes * 60 + seconds
                m = re.match(r"(\d+(?:\.\d+)?)\s*ms", text)
                if m:
                    return float(m.group(1)) / 1000.0
            except Exception:
                continue
    return None


def wait_for_cell_done(page, notebook_path, timeout=300, poll_interval=5):
    """Wait for cell execution to complete by polling the saved notebook file.

    Periodically triggers Ctrl+S to save the notebook, then reads the .ipynb
    JSON to check if the completion marker or an error appeared in the output.

    Returns a tuple (success, exec_time) where success is True/False and
    exec_time is the VS Code-reported execution time in seconds (or wall-clock
    elapsed time as a fallback).
    """
    print(f"  Waiting for cell execution to complete (up to {timeout}s)...")
    start = time.time()

    while time.time() - start < timeout:
        time.sleep(poll_interval)

        # Save the notebook so output is flushed to disk
        page.keyboard.press("Control+S")
        time.sleep(2)

        result = check_notebook_output(notebook_path)
        elapsed = time.time() - start

        if result is True:
            # Extract the execution time from VS Code's UI
            ui_time = extract_cell_execution_time(page)
            if ui_time is not None:
                print(f"  Cell succeeded (marker found). VS Code execution time: {ui_time:.1f}s")
            else:
                ui_time = elapsed
                print(f"  Cell succeeded (marker found). Wall-clock time: {ui_time:.1f}s (UI time not found)")
            return True, ui_time
        elif result is False:
            ui_time = extract_cell_execution_time(page)
            if ui_time is None:
                ui_time = elapsed
            print(f"  Cell failed after {ui_time:.1f}s (error found in output).")
            return False, ui_time
        else:
            print(f"  Still running ({elapsed:.0f}s elapsed)...")

    print(f"  Timed out after {timeout}s, assuming failure.")
    return False, timeout


def launch_app(app_config):
    """Launch the editor app with Chrome DevTools Protocol enabled."""
    cmd = app_config["command"]
    label = app_config["label"]
    print(f"Launching {label} with --remote-debugging-port={DEBUG_PORT}...")
    subprocess.Popen(
        [cmd, f"--remote-debugging-port={DEBUG_PORT}"],
        shell=True,
    )
    time.sleep(5)


def connect_to_app(pw, app_config, retries=5, delay=3):
    """Connect to the editor app via CDP. Retries if the debug port isn't ready yet."""
    label = app_config["label"]
    window_titles = app_config["window_titles"]
    for attempt in range(1, retries + 1):
        try:
            print(f"  Connecting to {label} via CDP (attempt {attempt}/{retries})...")
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{DEBUG_PORT}")
            # Find the main editor window
            for context in browser.contexts:
                for p in context.pages:
                    title = p.title() or ""
                    if any(wt in title for wt in window_titles):
                        print(f"  Connected to: {title}")
                        return browser, p
            # Fallback to first page
            page = browser.contexts[0].pages[0]
            print(f"  Connected to: {page.title()}")
            return browser, page
        except Exception as e:
            if attempt < retries:
                print(f"  Connection failed ({e}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Could not connect to {label} CDP after {retries} attempts: {e}")


def automate_vscode(page, run_number=1):
    """Run the full automation sequence. Returns True if the cell succeeded."""
    pyautogui.PAUSE = 0.3
    pyautogui.FAILSAFE = True

    os.makedirs(NOTEBOOK_SAVE_DIR, exist_ok=True)
    notebook_path = os.path.join(NOTEBOOK_SAVE_DIR, f"spark_test_{run_number}.ipynb")
    if os.path.exists(notebook_path):
        os.remove(notebook_path)

    # Step 2: Create new Jupyter Notebook via command palette
    print("Creating new Jupyter Notebook...")
    run_command_palette(page, "Create: New Jupyter Notebook")
    time.sleep(3)

    # Dismiss any kernel picker dialog
    page.keyboard.press("Escape")
    time.sleep(1)

    # Ensure the cell is in edit mode
    page.keyboard.press("Enter")
    time.sleep(0.5)

    # Step 3: Paste PySpark code into the cell
    print("Pasting PySpark code...")
    pyperclip.copy(PYSPARK_CODE)
    page.keyboard.press("Control+V")
    time.sleep(2)

    # Save the notebook to a known path so we can read its outputs later.
    # Ctrl+S on an untitled notebook opens a native Save As dialog — use pyautogui for that.
    print(f"Saving notebook to {notebook_path}...")
    page.keyboard.press("Control+S")
    time.sleep(2)
    pyperclip.copy(notebook_path)
    pyautogui.hotkey("ctrl", "a")
    time.sleep(0.3)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(0.5)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 4: Open kernel picker via command palette
    print("Opening kernel picker...")
    run_command_palette(page, "Notebook: Select Notebook Kernel")
    time.sleep(1)

    # Step 5: Select "Select Another Kernel" (if present)
    rows = page.locator(".quick-input-list .monaco-list-row").all()
    options = [row.inner_text(timeout=2000) for row in rows]
    if any("Select Another Kernel" in opt for opt in options):
        print("Selecting 'Select Another Kernel'...")
        select_from_quick_pick(page, "Select Another Kernel")
        time.sleep(1)
    else:
        print("'Select Another Kernel' not found, skipping step 5.")

    # Step 6: Select "Remote Spark Kernel"
    print("Selecting 'Remote Spark Kernel'...")
    select_from_quick_pick(page, "Remote Spark Kernel")
    time.sleep(1)

    # Step 7: Wait for the kernel list to populate, then select the first one.
    # This is where Playwright shines — we wait for actual DOM elements to appear
    # instead of doing flaky screenshot comparisons.
    print("Waiting for kernel list to populate (this may take a while)...")
    first_kernel = page.locator(".quick-input-list .monaco-list-row").first
    first_kernel.wait_for(state="visible", timeout=120_000)
    print("  Kernel list populated.")
    time.sleep(1)
    first_kernel.click()

    # Wait for kernel to finish connecting
    print("  Waiting for kernel to connect...")
    # The quick-input should close once a kernel is selected; wait for it to disappear.
    page.locator(".quick-input-widget").wait_for(state="hidden", timeout=120_000)
    print("  Kernel connected.")
    time.sleep(2)

    # Step 8: Execute all cells
    print("Executing all cells...")
    run_command_palette(page, "Notebook: Run All")

    # Steps 9-10: Wait for execution to complete and check result
    print("Waiting for notebook execution to complete...")
    success, exec_time = wait_for_cell_done(page, notebook_path, timeout=300, poll_interval=5)

    return success, exec_time


def close_app(app_config):
    """Fully close the editor app."""
    label = app_config["label"]
    process_name = app_config["process_name"]
    print(f"Closing {label}...")
    subprocess.run(f"taskkill /IM {process_name} /F", shell=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_DISPLAY_REQUIRED = 0x00000002


def prevent_sleep():
    """Tell Windows to stay awake (prevent sleep and display off)."""
    ctypes.windll.kernel32.SetThreadExecutionState(
        ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
    )
    print("Sleep prevention enabled.")


def allow_sleep():
    """Restore normal Windows sleep behavior."""
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
    print("Sleep prevention disabled.")


GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GSHEETS_TOKEN_PATH = os.path.join(os.path.dirname(__file__), "token.json")
GSHEETS_CREDS_PATH = os.path.join(os.path.dirname(__file__), "credentials.json")


def gsheets_login():
    """Interactive OAuth login flow. Saves token.json for future use."""
    if not os.path.exists(GSHEETS_CREDS_PATH):
        print(f"ERROR: {GSHEETS_CREDS_PATH} not found.")
        print("Download OAuth client credentials from Google Cloud Console")
        print("and save as credentials.json in the project directory.")
        sys.exit(1)
    flow = InstalledAppFlow.from_client_secrets_file(GSHEETS_CREDS_PATH, GSHEETS_SCOPES)
    creds = flow.run_local_server(port=0)
    with open(GSHEETS_TOKEN_PATH, "w") as f:
        f.write(creds.to_json())
    print(f"Login successful. Token saved to {GSHEETS_TOKEN_PATH}")


def get_gsheets_client():
    """Return a gspread client using a saved token. Does not trigger interactive login."""
    if not os.path.exists(GSHEETS_TOKEN_PATH):
        return None
    try:
        creds = Credentials.from_authorized_user_file(GSHEETS_TOKEN_PATH, GSHEETS_SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(GSHEETS_TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
        if not creds.valid:
            return None
        return gspread.authorize(creds)
    except Exception:
        return None


def validate_gsheets_token():
    """Check that a valid Google Sheets token exists. Exit if not."""
    client = get_gsheets_client()
    if client is None:
        print("ERROR: No valid Google Sheets token found.")
        print("Run 'python automate.py login' first to authenticate.")
        sys.exit(1)
    print("Google Sheets token is valid.")


def append_to_google_sheet(sheet_id, row_data):
    """Append a single row to the first worksheet of the given Google Sheet."""
    try:
        client = get_gsheets_client()
        if client is None:
            return
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.sheet1
        # Add header if sheet is empty
        if worksheet.row_count == 0 or not worksheet.cell(1, 1).value:
            worksheet.append_row(["Date", "Time", "IDE", "Status",
                                  "Cell Execution Time (s)", "Total Time (s)", "Recording"])
        worksheet.append_row(row_data, insert_data_option=InsertDataOption.insert_rows)
    except Exception as e:
        print(f"  Warning: failed to write to Google Sheet: {e}")


def create_grid_video(video_paths, output_path):
    """Combine multiple videos into a single grid video using ffmpeg.

    Arranges videos in a grid (e.g., 2x2 for 4 videos, 2x1 for 2, etc.)
    with all videos playing simultaneously.
    """
    n = len(video_paths)
    if n == 0:
        print("No videos to combine.")
        return
    if n == 1:
        print("Only one video, skipping grid creation.")
        return

    cols = math.ceil(math.sqrt(n))
    rows = math.ceil(n / cols)

    # Probe the first video for resolution
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height", "-of", "csv=p=0",
         video_paths[0]],
        capture_output=True, text=True,
    )
    try:
        src_w, src_h = [int(x) for x in probe.stdout.strip().split(",")]
    except ValueError:
        src_w, src_h = 1920, 1080
    cell_w = src_w // cols
    cell_h = src_h // rows
    # Ensure even dimensions for libx264
    cell_w -= cell_w % 2
    cell_h -= cell_h % 2

    print(f"\nCreating {cols}x{rows} grid video from {n} recordings ({cell_w}x{cell_h} per cell)...")

    # Build ffmpeg inputs
    inputs = []
    for path in video_paths:
        inputs.extend(["-i", path])

    # Scale each video to cell size; pad empty slots with black
    filter_parts = []
    total_slots = rows * cols
    for i in range(n):
        filter_parts.append(f"[{i}:v]scale={cell_w}:{cell_h},setsar=1[v{i}]")
    for i in range(n, total_slots):
        filter_parts.append(f"color=black:s={cell_w}x{cell_h}:d=1[v{i}]")

    # Build xstack layout string
    layout_parts = []
    for idx in range(total_slots):
        col = idx % cols
        row = idx // cols
        layout_parts.append(f"{cell_w * col}_{cell_h * row}")

    inputs_str = "".join(f"[v{i}]" for i in range(total_slots))
    filter_parts.append(
        f"{inputs_str}xstack=inputs={total_slots}:layout={'|'.join(layout_parts)}[out]"
    )

    cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", ";".join(filter_parts),
        "-map", "[out]",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-shortest",
        output_path,
    ]

    print(f"  Output: {output_path}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("  Grid video created successfully.")
    else:
        print(f"  Error creating grid video:\n{result.stderr[-500:]}")


def main():
    parser = argparse.ArgumentParser(description="Automate VS Code / Antigravity Jupyter Notebook creation with screen recording")
    subparsers = parser.add_subparsers(dest="command")

    # Login subcommand
    subparsers.add_parser("login", help="Authenticate with Google Sheets (interactive OAuth login)")

    # Run subcommand (default)
    run_parser = subparsers.add_parser("run", help="Run the automation (default)")
    run_parser.add_argument("--output-dir", default="output", help="Output directory for recordings (default: output)")
    run_parser.add_argument("-n", type=int, default=1, help="Number of times to run the automation (default: 1)")
    run_parser.add_argument("--loop", action="store_true",
                            help="Run forever, building a grid video every 9 runs")
    run_parser.add_argument("--app", choices=["vscode", "antigravity"], default="vscode",
                            help="Which editor to automate (default: vscode)")
    run_parser.add_argument("--sheet-id", default=None,
                            help="Google Sheet ID to append results to (optional)")

    args = parser.parse_args()

    # Default to "run" if no subcommand given
    if args.command is None:
        args = run_parser.parse_args()
        args.command = "run"

    if args.command == "login":
        gsheets_login()
        return

    app_config = APP_CONFIGS[args.app]
    validate_dependencies(app_config)

    if args.sheet_id:
        validate_gsheets_token()
    os.makedirs(args.output_dir, exist_ok=True)

    results = []
    prevent_sleep()

    ide_label = app_config["label"]
    history_header = "Date\tTime\tIDE\tStatus\tCell Execution Time (s)\tTotal Time (s)\tRecording"
    history_path = os.path.join(args.output_dir, "history.txt")

    def print_summary(batch_results):
        """Print a summary table for a batch of results."""
        header = "Date\tTime\tIDE\tStatus\tCell Execution Time (s)\tTotal Time (s)\tRecording"
        print(f"\n{'='*60}")
        print("  RESULTS SUMMARY (tab-separated, copy/paste into Google Sheets)")
        print(f"{'='*60}\n")
        print(header)
        for status, path, exec_time, total_time, start_date, start_time in batch_results:
            cell_str = f"{exec_time:.1f}" if exec_time is not None else "N/A"
            total_str = f"{total_time:.1f}"
            print(f"{start_date}\t{start_time}\t{ide_label}\t{status}\t{cell_str}\t{total_str}\t{path}")
        passed = sum(1 for s, _, _, _, _, _ in batch_results if s == "PASS")
        exec_times = [t for _, _, t, _, _, _ in batch_results if t is not None]
        total_times = [t for _, _, _, t, _, _ in batch_results]
        print()
        print(f"  {passed}/{len(batch_results)} passed")
        if exec_times:
            print(f"  Average cell execution time: {sum(exec_times) / len(exec_times):.1f}s")
        if total_times:
            print(f"  Average total time: {sum(total_times) / len(total_times):.1f}s")
        print(f"\n{'='*60}\n")

    def build_grid_video(batch_results, app_name):
        """Create a grid video from a batch of results."""
        video_paths = [path for _, path, _, _, _, _ in batch_results if os.path.exists(path)]
        if len(video_paths) > 1:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            grid_dir = os.path.join(args.output_dir, datetime.now().strftime("%Y-%m-%d"))
            os.makedirs(grid_dir, exist_ok=True)
            grid_path = os.path.join(grid_dir, f"{timestamp}_{app_name}_{len(video_paths)}runs.mp4")
            create_grid_video(video_paths, grid_path)

    def run_once(pw, run_number):
        """Execute a single automation run. Returns a result tuple."""
        date_dir = os.path.join(args.output_dir, datetime.now().strftime("%Y-%m-%d"))
        os.makedirs(date_dir, exist_ok=True)
        output_path = os.path.join(date_dir, f"recording_{datetime.now().strftime('%H%M%S')}.mp4")

        close_app(app_config)
        launch_app(app_config)
        browser, page = connect_to_app(pw, app_config)

        ffmpeg_proc = start_recording(output_path)
        success = False
        exec_time = None
        run_start_date = datetime.now().strftime("%Y-%m-%d")
        run_start_time = datetime.now().strftime("%H:%M:%S")
        total_start = time.time()
        try:
            success, exec_time = automate_vscode(page, run_number=run_number)
            time.sleep(2)
        except Exception as e:
            print(f"Error during run {run_number}: {e}")
        finally:
            total_time = time.time() - total_start
            if not success:
                log_path = output_path.replace(".mp4", "_jupyter_server.log")
                try:
                    capture_jupyter_server_log(page, log_path)
                except Exception as e:
                    print(f"  Warning: failed to capture log: {e}")
            stop_recording(ffmpeg_proc)
            try:
                browser.close()
            except Exception:
                pass

        status = "PASS" if success else "FAIL"
        result = (status, output_path, exec_time, total_time, run_start_date, run_start_time)
        print(f"\nRun {run_number}: {status} -> {output_path}")

        # Append to history file immediately
        cell_str = f"{exec_time:.1f}" if exec_time is not None else "N/A"
        total_str = f"{total_time:.1f}"
        write_header = not os.path.exists(history_path)
        with open(history_path, "a", encoding="utf-8") as f:
            if write_header:
                f.write(history_header + "\n")
            f.write(f"{run_start_date}\t{run_start_time}\t{ide_label}\t{status}\t{cell_str}\t{total_str}\t{output_path}\n")

        # Append to Google Sheet if configured
        if args.sheet_id:
            append_to_google_sheet(args.sheet_id, [
                run_start_date, run_start_time, ide_label, status, cell_str, total_str, output_path
            ])

        return result

    try:
        with sync_playwright() as pw:
            if args.loop:
                batch = []
                run_number = 1
                while True:
                    print(f"\n{'='*60}")
                    print(f"  Run {run_number} (loop mode, {app_config['label']})")
                    print(f"{'='*60}\n")

                    result = run_once(pw, run_number)
                    results.append(result)
                    batch.append(result)

                    if len(batch) == 9:
                        print_summary(batch)
                        build_grid_video(batch, args.app)
                        batch = []

                    run_number += 1
            else:
                for i in range(1, args.n + 1):
                    print(f"\n{'='*60}")
                    print(f"  Run {i} of {args.n} ({app_config['label']})")
                    print(f"{'='*60}\n")

                    result = run_once(pw, i)
                    results.append(result)

                print_summary(results)
                build_grid_video(results, args.app)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        if results:
            # Print summary of whatever we completed
            remaining = results[len(results) - len(results) % 9:] if args.loop else results
            if remaining:
                print_summary(remaining)

    # Close the IDE after the last run
    close_app(app_config)
    allow_sleep()


if __name__ == "__main__":
    main()
