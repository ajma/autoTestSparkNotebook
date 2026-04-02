"""Automate VS Code Jupyter Notebook creation with screen recording."""

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
        "command": "ag",
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

    # Step 5: Select "Select Another Kernel"
    print("Selecting 'Select Another Kernel'...")
    select_from_quick_pick(page, "Select Another Kernel")
    time.sleep(1)

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


def start_mouse_jiggler(interval=60):
    """Start a background thread that jiggles the mouse to prevent sleep."""
    stop_event = threading.Event()

    def _jiggle():
        while not stop_event.is_set():
            stop_event.wait(interval)
            if not stop_event.is_set():
                x, y = pyautogui.position()
                pyautogui.moveRel(1, 0, duration=0.05)
                pyautogui.moveRel(-1, 0, duration=0.05)

    thread = threading.Thread(target=_jiggle, daemon=True)
    thread.start()
    print(f"Mouse jiggler started (every {interval}s).")
    return stop_event


def stop_mouse_jiggler(stop_event):
    """Stop the mouse jiggler thread."""
    stop_event.set()
    print("Mouse jiggler stopped.")


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
    parser.add_argument("--output-dir", default="output", help="Output directory for recordings (default: output)")
    parser.add_argument("-n", type=int, default=1, help="Number of times to run the automation (default: 1)")
    parser.add_argument("--app", choices=["vscode", "antigravity"], default="vscode",
                        help="Which editor to automate (default: vscode)")
    args = parser.parse_args()

    app_config = APP_CONFIGS[args.app]
    validate_dependencies(app_config)
    os.makedirs(args.output_dir, exist_ok=True)

    results = []
    jiggler_stop = start_mouse_jiggler(interval=60)

    with sync_playwright() as pw:
        for i in range(1, args.n + 1):
            print(f"\n{'='*60}")
            print(f"  Run {i} of {args.n} ({app_config['label']})")
            print(f"{'='*60}\n")

            output_path = os.path.join(args.output_dir, f"recording_{i}.mp4")

            # Ensure clean state: kill any existing instance, then launch with CDP
            close_app(app_config)
            launch_app(app_config)
            browser, page = connect_to_app(pw, app_config)

            ffmpeg_proc = start_recording(output_path)
            success = False
            exec_time = None
            total_start = time.time()
            try:
                success, exec_time = automate_vscode(page, run_number=i)
                time.sleep(2)
            except Exception as e:
                print(f"Error during run {i}: {e}")
            finally:
                total_time = time.time() - total_start
                stop_recording(ffmpeg_proc)
                try:
                    browser.close()
                except Exception:
                    pass

            status = "PASS" if success else "FAIL"
            results.append((i, status, output_path, exec_time, total_time))
            print(f"\nRun {i}: {status} -> {output_path}")

    # Close the IDE after the last run
    close_app(app_config)
    stop_mouse_jiggler(jiggler_stop)

    # Print summary as tab-separated values (paste-friendly for Google Sheets)
    print(f"\n{'='*60}")
    print("  RESULTS SUMMARY (tab-separated, copy/paste into Google Sheets)")
    print(f"{'='*60}\n")
    print("Run\tStatus\tCell Execution Time (s)\tTotal Time (s)\tRecording")
    for run_num, status, path, exec_time, total_time in results:
        cell_str = f"{exec_time:.1f}" if exec_time is not None else "N/A"
        total_str = f"{total_time:.1f}"
        print(f"{run_num}\t{status}\t{cell_str}\t{total_str}\t{path}")

    passed = sum(1 for _, s, _, _, _ in results if s == "PASS")
    exec_times = [t for _, _, _, t, _ in results if t is not None]
    total_times = [t for _, _, _, _, t in results]
    print()
    print(f"  {passed}/{args.n} passed")
    if exec_times:
        avg_exec = sum(exec_times) / len(exec_times)
        print(f"  Average cell execution time: {avg_exec:.1f}s")
    if total_times:
        avg_total = sum(total_times) / len(total_times)
        print(f"  Average total time: {avg_total:.1f}s")
    print(f"\n{'='*60}\n")

    # Combine all recordings into a grid video
    video_paths = [path for _, _, path, _, _ in results if os.path.exists(path)]
    if len(video_paths) > 1:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        grid_path = os.path.join(args.output_dir, f"{timestamp}_{args.n}runs.mp4")
        create_grid_video(video_paths, grid_path)


if __name__ == "__main__":
    main()
