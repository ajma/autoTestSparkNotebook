"""Unattended automation tool that launches VS Code or Antigravity, creates a Jupyter notebook, connects to a remote Spark kernel via the Data Cloud Extension, executes PySpark code, records the screen, and logs results to Google Sheets."""

import argparse
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

# Platform detection
IS_WINDOWS = sys.platform == "win32"
IS_MACOS = sys.platform == "darwin"
IS_LINUX = sys.platform.startswith("linux")

# Platform-specific imports
if IS_WINDOWS:
    import ctypes
    import msvcrt
else:
    import select
    import termios
    import tty

# Modifier key: Cmd on macOS, Ctrl on Windows/Linux
MOD_KEY = "Meta" if IS_MACOS else "Control"       # Playwright keyboard
PYAUTOGUI_MOD = "command" if IS_MACOS else "ctrl"  # pyautogui hotkey

from google import genai
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from gspread.utils import InsertDataOption
import pyautogui
import pyperclip
from playwright.sync_api import sync_playwright

NOTEBOOK_SAVE_DIR = os.path.join(os.path.expanduser("~"), "spark_test_notebooks")

_BANNER_LINES = [
    ("\033[91m",   r"            _      _____       _   ___                _   "),
    ("\033[38;5;208m", r"  __ _ _  _| |_ __|_   _|__ __| |_/ __|_ __  __ _ _ _| |__"),
    ("\033[33m",   r" / _` | || |  _/ _ \| |/ -_|_-<  _\__ \ '_ \/ _` | '_| / /"),
    ("\033[32m",   r" \__,_|\_,_|\__\___/|_|\___/__/\__|___/ .__/\__,_|_| |_\_\ "),
    ("\033[32m",   r"                                      |_|                  "),
    ("\033[36m",   r"           _  _     _       _              _              "),
    ("\033[34m",   r"          | \| |___| |_ ___| |__  ___  ___| |__          "),
    ("\033[35m",   r"          | .` / _ \  _/ -_) '_ \/ _ \/ _ \ / /          "),
    ("\033[95m",   r"          |_|\_\___/\__\___|_.__/\___/\___/_\_\           "),
]
_BANNER_SUBTITLE = "\033[96mData Cloud Extension\033[0m \033[2m· Unattended Mode\033[0m"
_BANNER_SUBTITLE_LEN = len("Data Cloud Extension · Unattended Mode")


def _build_banner():
    w = 80
    art_width = max(len(text) for _, text in _BANNER_LINES)
    RST = "\033[0m"
    DIM_CLR = "\033[2m"
    lines = []
    lines.append(f"{DIM_CLR}{'=' * w}{RST}")
    lines.append(f"{DIM_CLR}|{' ' * (w - 2)}|{RST}")
    for color, text in _BANNER_LINES:
        padded = text.ljust(art_width)
        pad_total = w - 4 - art_width
        pad_left = pad_total // 2
        pad_right = pad_total - pad_left
        lines.append(f"{DIM_CLR}|{RST} {' ' * pad_left}{color}{padded}{RST}{' ' * pad_right} {DIM_CLR}|{RST}")
    lines.append(f"{DIM_CLR}|{' ' * (w - 2)}|{RST}")
    sub_pad_total = w - 4 - _BANNER_SUBTITLE_LEN
    sub_left = sub_pad_total // 2
    sub_right = sub_pad_total - sub_left
    lines.append(f"{DIM_CLR}|{RST} {' ' * sub_left}{_BANNER_SUBTITLE}{' ' * sub_right} {DIM_CLR}|{RST}")
    lines.append(f"{DIM_CLR}|{' ' * (w - 2)}|{RST}")
    lines.append(f"{DIM_CLR}{'=' * w}{RST}")
    return "\n".join(lines)

# ANSI color codes
RST = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
GRN = "\033[92m"
YEL = "\033[93m"
BLU = "\033[94m"
MAG = "\033[95m"
CYN = "\033[96m"

# Event that signals "stop after the current run finishes"
stop_after_current_run = threading.Event()


def _esc_listener():
    """Background thread: poll for ESC key and set the stop event."""
    if IS_WINDOWS:
        while not stop_after_current_run.is_set():
            if msvcrt.kbhit():
                key = msvcrt.getch()
                if key == b'\x1b':  # ESC
                    print(f"\n{BOLD}{YEL}>>> ESC pressed — will stop after the current run finishes. <<<{RST}")
                    stop_after_current_run.set()
                    return
            time.sleep(0.1)
    else:
        fd = sys.stdin.fileno()
        if not os.isatty(fd):
            return
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while not stop_after_current_run.is_set():
                if select.select([sys.stdin], [], [], 0.1)[0]:
                    key = sys.stdin.read(1)
                    if key == '\x1b':  # ESC
                        print(f"\n{BOLD}{YEL}>>> ESC pressed — will stop after the current run finishes. <<<{RST}")
                        stop_after_current_run.set()
                        return
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

DEBUG_PORT = 9222

APP_CONFIGS = {
    "vscode": {
        "command": "code",
        "process_name": "Code.exe" if IS_WINDOWS else "code",
        "window_titles": ["Visual Studio Code", "Code"],
        "label": "VS Code",
    },
    "antigravity": {
        "command": "antigravity",
        "process_name": "Antigravity.exe" if IS_WINDOWS else "antigravity",
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
        if IS_WINDOWS:
            hint = "winget install Gyan.FFmpeg"
        elif IS_MACOS:
            hint = "brew install ffmpeg"
        else:
            hint = "sudo apt install ffmpeg"
        missing.append(f"ffmpeg (install via: {hint})")
    cmd = app_config["command"]
    label = app_config["label"]
    if not shutil.which(cmd):
        missing.append(f"{cmd} ({label} CLI - ensure {label} is installed and on PATH)")
    if missing:
        print(f"{RED}Missing dependencies:{RST}")
        for dep in missing:
            print(f"  {RED}- {dep}{RST}")
        sys.exit(1)


def _get_screen_size():
    """Get screen resolution for Linux x11grab capture."""
    try:
        output = subprocess.check_output(
            ["xdpyinfo"], text=True, stderr=subprocess.DEVNULL
        )
        match = re.search(r"dimensions:\s+(\d+x\d+)", output)
        if match:
            return match.group(1)
    except Exception:
        pass
    return "1920x1080"


def start_recording(output_path):
    print(f"{CYN}Starting screen recording{RST} -> {DIM}{output_path}{RST}")
    if IS_WINDOWS:
        capture_args = ["-f", "gdigrab", "-framerate", "30", "-i", "desktop"]
    elif IS_MACOS:
        capture_args = ["-f", "avfoundation", "-framerate", "30",
                        "-capture_cursor", "1", "-i", "1:none"]
    else:
        screen_size = _get_screen_size()
        display = os.environ.get("DISPLAY", ":0.0")
        capture_args = ["-f", "x11grab", "-framerate", "30",
                        "-video_size", screen_size, "-i", display]
    proc = subprocess.Popen(
        [
            "ffmpeg", "-y",
            *capture_args,
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
    print(f"{CYN}Stopping screen recording...{RST}")
    try:
        proc.stdin.write(b"q")
        proc.stdin.flush()
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print(f"{YEL}Warning: ffmpeg did not stop gracefully, killing process.{RST}")
        proc.kill()
    except Exception as e:
        print(f"{YEL}Warning: error stopping recording: {e}{RST}")
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
    page.keyboard.press(f"{MOD_KEY}+P")
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
    page.keyboard.press(f"{MOD_KEY}+A")
    time.sleep(0.3)
    page.keyboard.press(f"{MOD_KEY}+C")
    time.sleep(0.5)

    log_text = pyperclip.paste()
    if log_text and log_text.strip():
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log_text)
        print(f"  {CYN}Jupyter Server log saved to{RST} {DIM}{log_path}{RST}")
    else:
        print(f"  {YEL}Warning: Jupyter Server log was empty.{RST}")


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


def wait_for_cell_done(page, notebook_path, timeout=300, poll_interval=3):
    """Wait for cell execution to complete by polling the saved notebook file.

    Periodically triggers Ctrl+S to save the notebook, then reads the .ipynb
    JSON to check if the completion marker or an error appeared in the output.

    Returns a tuple (success, exec_time) where success is True/False and
    exec_time is the VS Code-reported execution time in seconds (or wall-clock
    elapsed time as a fallback).
    """
    print(f"  {CYN}Waiting for cell execution to complete (up to {timeout}s)...{RST}")
    start = time.time()

    spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    rainbow = ["\033[91m", "\033[93m", "\033[92m", "\033[96m", "\033[94m", "\033[95m"]
    spin_idx = 0
    next_poll = start + poll_interval

    poll_result = [None]  # shared with background thread: True / False / None
    poll_thread = None

    def _poll_notebook():
        time.sleep(2)
        poll_result[0] = check_notebook_output(notebook_path)

    while time.time() - start < timeout:
        now = time.time()
        elapsed = now - start

        if now >= next_poll and (poll_thread is None or not poll_thread.is_alive()):
            result = poll_result[0]
            if result is True:
                ui_time = extract_cell_execution_time(page)
                if ui_time is not None:
                    print(f"\r  {GRN}Cell succeeded (marker found). VS Code execution time: {ui_time:.1f}s{RST}    ")
                else:
                    ui_time = elapsed
                    print(f"\r  {GRN}Cell succeeded (marker found). Wall-clock time: {ui_time:.1f}s{RST} {DIM}(UI time not found){RST}    ")
                return True, ui_time
            elif result is False:
                ui_time = extract_cell_execution_time(page)
                if ui_time is None:
                    ui_time = elapsed
                print(f"\r  {RED}Cell failed after {ui_time:.1f}s (error found in output).{RST}    ")
                return False, ui_time

            page.keyboard.press(f"{MOD_KEY}+S")
            poll_result[0] = None
            poll_thread = threading.Thread(target=_poll_notebook, daemon=True)
            poll_thread.start()
            next_poll = now + poll_interval

        color = rainbow[spin_idx % len(rainbow)]
        print(f"\r  {DIM}Still running ({elapsed:.0f}s elapsed)... {color}{spinner[spin_idx % len(spinner)]}{RST}    ", end="", flush=True)
        spin_idx += 1
        time.sleep(0.08)

    print(f"\r  {RED}Timed out after {timeout}s, assuming failure.{RST}    ")
    return False, timeout


def launch_app(app_config):
    """Launch the editor app with Chrome DevTools Protocol enabled."""
    cmd = app_config["command"]
    label = app_config["label"]
    print(f"{CYN}Launching {BOLD}{label}{RST}{CYN} with --remote-debugging-port={DEBUG_PORT}...{RST}")
    subprocess.Popen(
        [cmd, f"--remote-debugging-port={DEBUG_PORT}"],
        shell=IS_WINDOWS,
    )
    time.sleep(5)


def connect_to_app(pw, app_config, retries=5, delay=3):
    """Connect to the editor app via CDP. Retries if the debug port isn't ready yet."""
    label = app_config["label"]
    window_titles = app_config["window_titles"]
    for attempt in range(1, retries + 1):
        try:
            print(f"  {CYN}Connecting to {label} via CDP (attempt {attempt}/{retries})...{RST}")
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{DEBUG_PORT}")
            # Find the main editor window
            for context in browser.contexts:
                for p in context.pages:
                    title = p.title() or ""
                    if any(wt in title for wt in window_titles):
                        print(f"  {GRN}Connected to: {title}{RST}")
                        return browser, p
            # Fallback to first page
            page = browser.contexts[0].pages[0]
            print(f"  {GRN}Connected to: {page.title()}{RST}")
            return browser, page
        except Exception as e:
            if attempt < retries:
                print(f"  {YEL}Connection failed ({e}), retrying in {delay}s...{RST}")
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
    print(f"{CYN}Creating new Jupyter Notebook...{RST}")
    run_command_palette(page, "Create: New Jupyter Notebook")
    time.sleep(3)

    # Dismiss any kernel picker dialog
    page.keyboard.press("Escape")
    time.sleep(1)

    # Ensure the cell is in edit mode
    page.keyboard.press("Enter")
    time.sleep(0.5)

    # Step 3: Paste PySpark code into the cell
    print(f"{CYN}Pasting PySpark code...{RST}")
    pyperclip.copy(PYSPARK_CODE)
    page.keyboard.press(f"{MOD_KEY}+V")
    time.sleep(2)

    # Save the notebook to a known path so we can read its outputs later.
    # Ctrl/Cmd+S on an untitled notebook opens a native Save As dialog — use pyautogui for that.
    print(f"{CYN}Saving notebook to{RST} {DIM}{notebook_path}{RST}{CYN}...{RST}")
    page.keyboard.press(f"{MOD_KEY}+S")
    time.sleep(2)
    if IS_MACOS:
        # macOS Save dialog: Cmd+Shift+G to open "Go to Folder", navigate there
        pyautogui.hotkey("command", "shift", "g")
        time.sleep(1)
        pyperclip.copy(os.path.dirname(notebook_path))
        pyautogui.hotkey("command", "a")
        pyautogui.hotkey("command", "v")
        time.sleep(0.5)
        pyautogui.press("enter")
        time.sleep(1)
        # Set the filename
        pyperclip.copy(os.path.basename(notebook_path))
        pyautogui.hotkey("command", "a")
        pyautogui.hotkey("command", "v")
    else:
        # Windows/Linux: paste full path into filename field
        pyperclip.copy(notebook_path)
        pyautogui.hotkey(PYAUTOGUI_MOD, "a")
        time.sleep(0.3)
        pyautogui.hotkey(PYAUTOGUI_MOD, "v")
    time.sleep(0.5)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 4: Open kernel picker via command palette
    print(f"{CYN}Opening kernel picker...{RST}")
    run_command_palette(page, "Notebook: Select Notebook Kernel")
    time.sleep(1)

    # Step 5: Select "Select Another Kernel" (if present)
    rows = page.locator(".quick-input-list .monaco-list-row").all()
    options = [row.inner_text(timeout=2000) for row in rows]
    if any("Select Another Kernel" in opt for opt in options):
        print(f"{CYN}Selecting 'Select Another Kernel'...{RST}")
        select_from_quick_pick(page, "Select Another Kernel")
        time.sleep(1)
    else:
        print(f"{DIM}'Select Another Kernel' not found, skipping step 5.{RST}")

    # Step 6: Select "Remote Spark Kernel"
    print(f"{CYN}Selecting 'Remote Spark Kernel'...{RST}")
    select_from_quick_pick(page, "Remote Spark Kernel")
    time.sleep(1)

    # Step 7: Wait for the kernel list to populate, then select the first one.
    # This is where Playwright shines — we wait for actual DOM elements to appear
    # instead of doing flaky screenshot comparisons.
    print(f"{CYN}Waiting for kernel list to populate (this may take a while)...{RST}")
    first_kernel = page.locator(".quick-input-list .monaco-list-row").first
    first_kernel.wait_for(state="visible", timeout=120_000)
    print(f"  {GRN}Kernel list populated.{RST}")
    time.sleep(1)
    first_kernel.click()

    # Wait for kernel to finish connecting
    print(f"  {CYN}Waiting for kernel to connect...{RST}")
    # The quick-input should close once a kernel is selected; wait for it to disappear.
    page.locator(".quick-input-widget").wait_for(state="hidden", timeout=120_000)
    print(f"  {GRN}Kernel connected.{RST}")
    time.sleep(2)

    # Step 8: Execute all cells
    print(f"{CYN}Executing all cells...{RST}")
    run_command_palette(page, "Notebook: Run All")

    # Steps 9-10: Wait for execution to complete and check result
    print(f"{CYN}Waiting for notebook execution to complete...{RST}")
    success, exec_time = wait_for_cell_done(page, notebook_path, timeout=300, poll_interval=5)

    return success, exec_time


def close_app(app_config):
    """Fully close the editor app."""
    label = app_config["label"]
    process_name = app_config["process_name"]
    print(f"{CYN}Closing {label}...{RST}")
    if IS_WINDOWS:
        subprocess.run(f"taskkill /IM {process_name} /F", shell=True,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        subprocess.run(["pkill", "-f", process_name],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_DISPLAY_REQUIRED = 0x00000002

_caffeinate_proc = None


def prevent_sleep():
    """Prevent the system from sleeping."""
    global _caffeinate_proc
    if IS_WINDOWS:
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
        )
    elif IS_MACOS:
        _caffeinate_proc = subprocess.Popen(
            ["caffeinate", "-dims"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        subprocess.run(["xset", "s", "off", "-dpms"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{GRN}Sleep prevention enabled.{RST}")


def allow_sleep():
    """Restore normal sleep behavior."""
    global _caffeinate_proc
    if IS_WINDOWS:
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
    elif IS_MACOS:
        if _caffeinate_proc:
            _caffeinate_proc.terminate()
            _caffeinate_proc = None
    else:
        subprocess.run(["xset", "s", "on", "+dpms"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{DIM}Sleep prevention disabled.{RST}")


GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GSHEETS_TOKEN_PATH = os.path.join(os.path.dirname(__file__), "token.json")
GSHEETS_CREDS_PATH = os.path.join(os.path.dirname(__file__), "credentials.json")


def gsheets_login():
    """Interactive OAuth login flow. Saves token.json for future use."""
    if not os.path.exists(GSHEETS_CREDS_PATH):
        print(f"{RED}ERROR: {GSHEETS_CREDS_PATH} not found.{RST}")
        print(f"{RED}Download OAuth client credentials from Google Cloud Console{RST}")
        print(f"{RED}and save as credentials.json in the project directory.{RST}")
        sys.exit(1)
    flow = InstalledAppFlow.from_client_secrets_file(GSHEETS_CREDS_PATH, GSHEETS_SCOPES)
    creds = flow.run_local_server(port=0)
    with open(GSHEETS_TOKEN_PATH, "w") as f:
        f.write(creds.to_json())
    print(f"{GRN}Login successful.{RST} Token saved to {DIM}{GSHEETS_TOKEN_PATH}{RST}")


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
        print(f"{RED}ERROR: No valid Google Sheets token found.{RST}")
        print(f"{RED}Run 'python automate.py login' first to authenticate.{RST}")
        sys.exit(1)
    print(f"{GRN}Google Sheets token is valid.{RST}")


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
                                  "Cell Execution Time (s)", "Total Time (s)", "Recording",
                                  "Failure Summary"])
        worksheet.append_row(row_data, insert_data_option=InsertDataOption.insert_rows)
    except Exception as e:
        print(f"  {YEL}Warning: failed to write to Google Sheet: {e}{RST}")


def analyze_log_with_gemini(log_path, client):
    """Use Gemini to analyze a Jupyter server log and summarize the failure."""
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            log_text = f.read()
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=f"Analyze this Jupyter server log from a failed PySpark notebook run. "
            f"Summarize what went wrong in 150 characters or less. Be specific and concise. "
            f"Do not use markdown. Reply with ONLY the summary, nothing else.\n\n{log_text}",
        )
        summary = response.text.strip()
        return summary[:150]
    except Exception as e:
        print(f"  {YEL}Warning: Gemini analysis failed: {e}{RST}")
        return ""


def create_grid_video(video_paths, output_path):
    """Combine multiple videos into a single grid video using ffmpeg.

    Arranges videos in a grid (e.g., 2x2 for 4 videos, 2x1 for 2, etc.)
    with all videos playing simultaneously.
    """
    n = len(video_paths)
    if n == 0:
        print(f"{DIM}No videos to combine.{RST}")
        return
    if n == 1:
        print(f"{DIM}Only one video, skipping grid creation.{RST}")
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

    print(f"\n{MAG}Creating {cols}x{rows} grid video from {n} recordings ({cell_w}x{cell_h} per cell)...{RST}")

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

    print(f"  Output: {DIM}{output_path}{RST}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  {GRN}Grid video created successfully.{RST}")
    else:
        print(f"  {RED}Error creating grid video:\n{result.stderr[-500:]}{RST}")


def main():
    print(_build_banner())
    parser = argparse.ArgumentParser(
        description="Unattended automation tool that launches VS Code or Antigravity, creates a Jupyter notebook, connects to a remote Spark kernel via the Data Cloud Extension, executes PySpark code, records the screen, and logs results to Google Sheets.",
        formatter_class=lambda prog: argparse.HelpFormatter(prog, width=80))
    subparsers = parser.add_subparsers(dest="command", title="commands")

    # Login subcommand
    subparsers.add_parser("login", help="Authenticate with Google Sheets (interactive OAuth login)")

    # Run subcommand (default)
    run_parser = subparsers.add_parser("run", help="Run the automation")
    run_parser.add_argument("--output-dir", default="output", help="Output directory for recordings (default: output)")
    run_parser.add_argument("-n", type=int, default=1, help="Number of times to run the automation (default: 1)")
    run_parser.add_argument("--loop", action="store_true",
                            help="Run forever, building a grid video every 9 runs")
    run_parser.add_argument("--app", choices=["vscode", "antigravity"], default="vscode",
                            help="Which editor to automate (default: vscode)")
    args = parser.parse_args()

    # Show help if no subcommand given
    if args.command is None:
        parser.print_help()
        return

    if args.command == "login":
        gsheets_login()
        return

    # Load config
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    config = {}
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)
    sheet_id = config.get("sheet_id")

    app_config = APP_CONFIGS[args.app]
    validate_dependencies(app_config)

    gemini_api_key = config.get("gemini_api_key")
    gemini_client = None
    if gemini_api_key:
        try:
            gemini_client = genai.Client(api_key=gemini_api_key)
            gemini_client.models.generate_content(model="gemini-2.5-flash", contents="test")
            print(f"{GRN}Gemini API key is valid.{RST}")
        except Exception as e:
            print(f"{RED}ERROR: Gemini API key validation failed: {e}{RST}")
            sys.exit(1)
    if sheet_id:
        validate_gsheets_token()
    os.makedirs(args.output_dir, exist_ok=True)

    results = []
    prevent_sleep()

    ide_label = app_config["label"]
    history_header = "Date\tTime\tIDE\tStatus\tCell Execution Time (s)\tTotal Time (s)\tRecording\tFailure Summary"
    history_path = os.path.join(args.output_dir, "history.txt")

    def print_running_tally(all_results):
        passed = sum(1 for s, _, _, _, _, _, _ in all_results if s == "PASS")
        total = len(all_results)
        pct = (passed / total * 100) if total else 0
        color = GRN if passed == total else YEL
        print(f"  {color}{BOLD}{passed}/{total} passing ({pct:.0f}%){RST}")

    def print_summary(batch_results):
        """Print a summary table for a batch of results."""
        header = "Date\tTime\tIDE\tStatus\tCell Execution Time (s)\tTotal Time (s)\tRecording\tFailure Summary"
        print(f"\n{MAG}{'='*60}{RST}")
        print(f"  {BOLD}{MAG}RESULTS SUMMARY{RST}")
        print(f"{MAG}{'='*60}{RST}\n")
        print(f"{DIM}{header}{RST}")
        for status, path, exec_time, total_time, start_date, start_time, summary in batch_results:
            cell_str = f"{exec_time:.1f}" if exec_time is not None else "N/A"
            total_str = f"{total_time:.1f}"
            status_color = GRN if status == "PASS" else RED
            print(f"{start_date}\t{start_time}\t{ide_label}\t{status_color}{status}{RST}\t{cell_str}\t{total_str}\t{DIM}{path}{RST}\t{summary}")
        passed = sum(1 for s, _, _, _, _, _, _ in batch_results if s == "PASS")
        exec_times = [t for _, _, t, _, _, _, _ in batch_results if t is not None]
        total_times = [t for _, _, _, t, _, _, _ in batch_results]
        print()
        color = GRN if passed == len(batch_results) else YEL
        print(f"  {color}{BOLD}{passed}/{len(batch_results)} passed{RST}")
        if exec_times:
            print(f"  {BLU}Average cell execution time: {sum(exec_times) / len(exec_times):.1f}s{RST}")
        if total_times:
            print(f"  {BLU}Average total time: {sum(total_times) / len(total_times):.1f}s{RST}")
        print(f"\n{MAG}{'='*60}{RST}\n")

    def build_grid_video(batch_results, app_name):
        """Create a grid video from failed runs only."""
        video_paths = [path for status, path, _, _, _, _, _ in batch_results if status == "FAIL" and os.path.exists(path)]
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
        log_path = output_path.replace(".mp4", "_jupyter_server.log")
        success = False
        exec_time = None
        run_start_date = datetime.now().strftime("%Y-%m-%d")
        run_start_time = datetime.now().strftime("%H:%M:%S")
        total_start = time.time()
        try:
            success, exec_time = automate_vscode(page, run_number=run_number)
            time.sleep(2)
        except Exception as e:
            print(f"{RED}Error during run {run_number}: {e}{RST}")
        finally:
            total_time = time.time() - total_start
            if not success:
                try:
                    capture_jupyter_server_log(page, log_path)
                except Exception as e:
                    print(f"  {YEL}Warning: failed to capture log: {e}{RST}")
            stop_recording(ffmpeg_proc)
            try:
                browser.close()
            except Exception:
                pass

        status = "PASS" if success else "FAIL"
        failure_summary = ""
        # Delete video for passing runs to save disk space
        if success and os.path.exists(output_path):
            os.remove(output_path)
            output_path = ""
            print(f"\n{BOLD}{GRN}Run {run_number}: PASS{RST} {DIM}(video deleted){RST}")
        else:
            print(f"\n{BOLD}{RED}Run {run_number}: FAIL{RST} -> {DIM}{output_path}{RST}")
            # Analyze log with Gemini if available
            if gemini_client and os.path.exists(log_path):
                failure_summary = analyze_log_with_gemini(log_path, gemini_client)
                if failure_summary:
                    print(f"  {YEL}Failure summary: {failure_summary}{RST}")
        result = (status, output_path, exec_time, total_time, run_start_date, run_start_time, failure_summary)

        # Append to history file immediately
        cell_str = f"{exec_time:.1f}" if exec_time is not None else "N/A"
        total_str = f"{total_time:.1f}"
        write_header = not os.path.exists(history_path)
        with open(history_path, "a", encoding="utf-8") as f:
            if write_header:
                f.write(history_header + "\n")
            f.write(f"{run_start_date}\t{run_start_time}\t{ide_label}\t{status}\t{cell_str}\t{total_str}\t{output_path}\t{failure_summary}\n")

        # Append to Google Sheet if configured
        if sheet_id:
            append_to_google_sheet(sheet_id, [
                run_start_date, run_start_time, ide_label, status, cell_str, total_str, output_path, failure_summary
            ])

        return result

    # Start background thread listening for ESC key
    esc_thread = threading.Thread(target=_esc_listener, daemon=True)
    esc_thread.start()

    try:
        with sync_playwright() as pw:
            if args.loop:
                batch = []
                run_number = 1
                while True:
                    print(f"\n{BLU}{'='*60}{RST}")
                    print(f"  {BOLD}{BLU}Run {run_number}{RST} {CYN}(loop mode, {app_config['label']}){RST}")
                    print(f"{BLU}{'='*60}{RST}\n")

                    result = run_once(pw, run_number)
                    results.append(result)
                    batch.append(result)
                    print_running_tally(results)

                    if len(batch) == 9:
                        print_summary(batch)
                        build_grid_video(batch, args.app)
                        batch = []

                    run_number += 1

                    if stop_after_current_run.is_set():
                        print(f"\n{YEL}Stopping after ESC key press.{RST}")
                        if batch:
                            print_summary(batch)
                            build_grid_video(batch, args.app)
                        break
            else:
                for i in range(1, args.n + 1):
                    print(f"\n{BLU}{'='*60}{RST}")
                    print(f"  {BOLD}{BLU}Run {i} of {args.n}{RST} {CYN}({app_config['label']}){RST}")
                    print(f"{BLU}{'='*60}{RST}\n")

                    result = run_once(pw, i)
                    results.append(result)
                    print_running_tally(results)

                    if stop_after_current_run.is_set():
                        print(f"\n{YEL}Stopping after ESC key press.{RST}")
                        break

                print_summary(results)
                build_grid_video(results, args.app)
    except KeyboardInterrupt:
        print(f"\n\n{YEL}Interrupted by user.{RST}")
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
