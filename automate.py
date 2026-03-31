"""Automate VS Code Jupyter Notebook creation with screen recording."""

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import time

import pyautogui
import pyperclip
from playwright.sync_api import sync_playwright

NOTEBOOK_SAVE_DIR = os.path.join(os.path.expanduser("~"), "spark_test_notebooks")
DEBUG_PORT = 9222

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


def validate_dependencies():
    missing = []
    if not shutil.which("ffmpeg"):
        missing.append("ffmpeg (install via: winget install Gyan.FFmpeg)")
    if not shutil.which("code"):
        missing.append("code (VS Code CLI - ensure VS Code is installed and on PATH)")
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


def wait_for_cell_done(page, notebook_path, timeout=300, poll_interval=5):
    """Wait for cell execution to complete by polling the saved notebook file.

    Periodically triggers Ctrl+S to save the notebook, then reads the .ipynb
    JSON to check if the completion marker or an error appeared in the output.

    Returns a tuple (success, elapsed) where success is True/False and elapsed
    is the execution time in seconds.
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
            print(f"  Cell succeeded after {elapsed:.0f}s (marker found in output).")
            print(f"  Execution time: {elapsed:.1f}s")
            return True, elapsed
        elif result is False:
            print(f"  Cell failed after {elapsed:.0f}s (error found in output).")
            return False, elapsed
        else:
            print(f"  Still running ({elapsed:.0f}s elapsed)...")

    print(f"  Timed out after {timeout}s, assuming failure.")
    return False, timeout


def launch_vscode():
    """Launch VS Code with Chrome DevTools Protocol enabled."""
    print(f"Launching VS Code with --remote-debugging-port={DEBUG_PORT}...")
    subprocess.Popen(
        ["code", f"--remote-debugging-port={DEBUG_PORT}"],
        shell=True,
    )
    time.sleep(5)


def connect_to_vscode(pw, retries=5, delay=3):
    """Connect to VS Code via CDP. Retries if the debug port isn't ready yet."""
    for attempt in range(1, retries + 1):
        try:
            print(f"  Connecting to VS Code via CDP (attempt {attempt}/{retries})...")
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{DEBUG_PORT}")
            # Find the main VS Code editor window
            for context in browser.contexts:
                for p in context.pages:
                    title = p.title() or ""
                    if "Visual Studio Code" in title or "Code" in title:
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
                raise RuntimeError(f"Could not connect to VS Code CDP after {retries} attempts: {e}")


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


def close_vscode():
    """Fully close VS Code."""
    print("Closing VS Code...")
    subprocess.run("taskkill /IM Code.exe /F", shell=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


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
    parser = argparse.ArgumentParser(description="Automate VS Code Jupyter Notebook creation with screen recording")
    parser.add_argument("--output-dir", default="output", help="Output directory for recordings (default: output)")
    parser.add_argument("-n", type=int, default=1, help="Number of times to run the automation (default: 1)")
    args = parser.parse_args()

    validate_dependencies()
    os.makedirs(args.output_dir, exist_ok=True)

    results = []

    with sync_playwright() as pw:
        for i in range(1, args.n + 1):
            print(f"\n{'='*60}")
            print(f"  Run {i} of {args.n}")
            print(f"{'='*60}\n")

            output_path = os.path.join(args.output_dir, f"recording_{i}.mp4")

            # Ensure clean state: kill any existing VS Code, then launch with CDP
            close_vscode()
            launch_vscode()
            browser, page = connect_to_vscode(pw)

            ffmpeg_proc = start_recording(output_path)
            success = False
            exec_time = None
            try:
                success, exec_time = automate_vscode(page, run_number=i)
                time.sleep(2)
            except Exception as e:
                print(f"Error during run {i}: {e}")
            finally:
                stop_recording(ffmpeg_proc)
                try:
                    browser.close()
                except Exception:
                    pass

            status = "PASS" if success else "FAIL"
            results.append((i, status, output_path, exec_time))
            print(f"\nRun {i}: {status} -> {output_path}")

    # Print summary
    print(f"\n{'='*60}")
    print("  RESULTS SUMMARY")
    print(f"{'='*60}")
    for run_num, status, path, exec_time in results:
        time_str = f"  ({exec_time:.1f}s)" if exec_time is not None else ""
        print(f"  Run {run_num}: {status}{time_str}  ->  {path}")

    passed = sum(1 for _, s, _, _ in results if s == "PASS")
    exec_times = [t for _, _, _, t in results if t is not None]
    print(f"\n  {passed}/{args.n} passed")
    if exec_times:
        avg_time = sum(exec_times) / len(exec_times)
        print(f"  Average execution time: {avg_time:.1f}s")
    print(f"{'='*60}\n")

    # Combine all recordings into a grid video
    video_paths = [path for _, _, path, _ in results if os.path.exists(path)]
    if len(video_paths) > 1:
        grid_path = os.path.join(args.output_dir, "grid_all_runs.mp4")
        create_grid_video(video_paths, grid_path)


if __name__ == "__main__":
    main()
