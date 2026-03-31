"""Automate VS Code Jupyter Notebook creation with screen recording."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

import numpy as np
import pyautogui
import pyperclip

NOTEBOOK_SAVE_DIR = os.path.join(os.path.expanduser("~"), "spark_test_notebooks")

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
        print("Warning: ffmpeg did not stop gracefully, killing process. MP4 may be corrupted.")
        proc.kill()
    except Exception as e:
        print(f"Warning: error stopping recording: {e}")
        proc.kill()


def wait_for_stable_screen(timeout=120, stable_duration=5, poll_interval=2):
    """Wait until the screen stops changing, indicating loading is done.

    Args:
        timeout: Max seconds to wait before giving up.
        stable_duration: Seconds the screen must remain unchanged to be considered stable.
        poll_interval: Seconds between screenshot comparisons.
    """
    print(f"  Waiting for screen to stabilize (up to {timeout}s)...")
    prev_screenshot = np.array(pyautogui.screenshot())
    stable_since = None
    start = time.time()

    while time.time() - start < timeout:
        time.sleep(poll_interval)
        curr_screenshot = np.array(pyautogui.screenshot())
        diff = np.mean(np.abs(curr_screenshot.astype(int) - prev_screenshot.astype(int)))
        prev_screenshot = curr_screenshot

        if diff < 1.0:  # essentially unchanged
            if stable_since is None:
                stable_since = time.time()
            elif time.time() - stable_since >= stable_duration:
                elapsed = time.time() - start
                print(f"  Screen stable after {elapsed:.0f}s.")
                return
        else:
            stable_since = None

    print(f"  Timed out after {timeout}s, proceeding anyway.")


def wait_for_picker_populated(timeout=120, stable_duration=5, poll_interval=2):
    """Wait for the kernel picker list to populate.

    First captures a baseline screenshot, then waits for a significant visual
    change (indicating the kernels list has loaded), and finally waits for the
    screen to stabilize after that change.
    """
    print(f"  Waiting for kernel picker to populate (up to {timeout}s)...")
    baseline = np.array(pyautogui.screenshot())
    start = time.time()
    change_detected = False

    while time.time() - start < timeout:
        time.sleep(poll_interval)
        curr = np.array(pyautogui.screenshot())
        diff = np.mean(np.abs(curr.astype(int) - baseline.astype(int)))

        if not change_detected:
            if diff > 2.0:  # meaningful change from baseline
                elapsed = time.time() - start
                print(f"  Picker content changed after {elapsed:.0f}s, waiting to stabilize...")
                change_detected = True
                # Now wait for stability after the change
                prev = curr
                stable_since = time.time()
                while time.time() - start < timeout:
                    time.sleep(poll_interval)
                    curr2 = np.array(pyautogui.screenshot())
                    diff2 = np.mean(np.abs(curr2.astype(int) - prev.astype(int)))
                    prev = curr2
                    if diff2 < 1.0:
                        if time.time() - stable_since >= stable_duration:
                            elapsed = time.time() - start
                            print(f"  Picker stable after {elapsed:.0f}s.")
                            return
                    else:
                        stable_since = time.time()
                break

    print(f"  Timed out after {timeout}s, proceeding anyway.")


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
            # Check for error output type
            if output.get("output_type") == "error":
                return False
            # Check text output (stream or execute_result)
            text_parts = output.get("text", [])
            if isinstance(text_parts, str):
                text_parts = [text_parts]
            text = "".join(text_parts)
            if "EXECUTION_COMPLETE_MARKER" in text:
                return True
    return None


def wait_for_cell_done(notebook_path, timeout=300, poll_interval=5):
    """Wait for cell execution to complete by polling the saved notebook file.

    Periodically triggers Ctrl+S to save the notebook, then reads the .ipynb
    JSON to check if the completion marker or an error appeared in the output.

    Returns True if succeeded, False if failed or timed out.
    """
    print(f"  Waiting for cell execution to complete (up to {timeout}s)...")
    start = time.time()

    while time.time() - start < timeout:
        time.sleep(poll_interval)

        # Save the notebook so output is flushed to disk
        pyautogui.hotkey("ctrl", "s")
        time.sleep(2)

        result = check_notebook_output(notebook_path)
        elapsed = time.time() - start

        if result is True:
            print(f"  Cell succeeded after {elapsed:.0f}s (marker found in output).")
            return True
        elif result is False:
            print(f"  Cell failed after {elapsed:.0f}s (error found in output).")
            return False
        else:
            print(f"  Still running ({elapsed:.0f}s elapsed)...")

    print(f"  Timed out after {timeout}s, assuming failure.")
    return False


def automate_vscode(run_number=1):
    """Run the full automation sequence. Returns True if the cell succeeded, False otherwise."""
    pyautogui.PAUSE = 0.3
    pyautogui.FAILSAFE = True

    # Prepare the save path for this run's notebook
    os.makedirs(NOTEBOOK_SAVE_DIR, exist_ok=True)
    notebook_path = os.path.join(NOTEBOOK_SAVE_DIR, f"spark_test_{run_number}.ipynb")
    # Clean up any leftover file from a previous run
    if os.path.exists(notebook_path):
        os.remove(notebook_path)

    # Step 1: Open VS Code
    print("Opening VS Code...")
    subprocess.Popen("code", shell=True)
    time.sleep(5)

    # Step 2: Open Command Palette and create new Jupyter Notebook
    print("Creating new Jupyter Notebook...")
    pyautogui.hotkey("ctrl", "shift", "p")
    time.sleep(1)

    pyautogui.write("Create: New Jupyter Notebook", interval=0.03)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(5)

    # Dismiss any kernel picker dialog
    pyautogui.press("escape")
    time.sleep(1)

    # Ensure the cell is in edit mode by pressing Enter
    pyautogui.press("enter")
    time.sleep(0.5)

    # Step 3: Paste PySpark code into the cell
    print("Pasting PySpark code...")
    pyperclip.copy(PYSPARK_CODE)
    time.sleep(0.5)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(2)

    # Save the notebook to a known path so we can read its outputs later.
    # Ctrl+S on an untitled notebook opens a Save As dialog.
    print(f"Saving notebook to {notebook_path}...")
    pyautogui.hotkey("ctrl", "s")
    time.sleep(2)
    # Type the full path into the Save As dialog and confirm
    pyperclip.copy(notebook_path)
    pyautogui.hotkey("ctrl", "a")
    time.sleep(0.3)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(0.5)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 4: Click "Select Kernel" via Command Palette
    print("Opening kernel picker...")
    pyautogui.hotkey("ctrl", "shift", "p")
    time.sleep(1)
    pyautogui.write("Notebook: Select Notebook Kernel", interval=0.03)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 5: Click "Select Another Kernel"
    print("Selecting 'Select Another Kernel'...")
    pyautogui.write("Select Another Kernel", interval=0.03)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 6: Click "Remote Spark Kernel"
    print("Selecting 'Remote Spark Kernel'...")
    pyautogui.write("Remote Spark Kernel", interval=0.03)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 7: Select the first one in the list (might take a long time)
    print("Selecting first kernel in list (this may take a while)...")
    wait_for_picker_populated(timeout=120, stable_duration=5, poll_interval=2)
    pyautogui.press("enter")
    wait_for_stable_screen(timeout=120, stable_duration=5, poll_interval=2)

    # Step 8: Execute all cells
    print("Executing all cells...")
    pyautogui.hotkey("ctrl", "shift", "p")
    time.sleep(1)
    pyautogui.write("Notebook: Run All", interval=0.03)
    time.sleep(1)
    pyautogui.press("enter")

    # Steps 9-10: Wait for execution to complete and check result
    print("Waiting for notebook execution to complete...")
    success = wait_for_cell_done(notebook_path, timeout=300, poll_interval=5)

    return success


def close_vscode():
    """Fully close VS Code."""
    print("Closing VS Code...")
    subprocess.run("taskkill /IM Code.exe /F", shell=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


import math


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

    for i in range(1, args.n + 1):
        print(f"\n{'='*60}")
        print(f"  Run {i} of {args.n}")
        print(f"{'='*60}\n")

        output_path = os.path.join(args.output_dir, f"recording_{i}.mp4")
        ffmpeg_proc = start_recording(output_path)
        success = False
        try:
            success = automate_vscode(run_number=i)
            time.sleep(2)
        except Exception as e:
            print(f"Error during run {i}: {e}")
        finally:
            stop_recording(ffmpeg_proc)

        status = "PASS" if success else "FAIL"
        results.append((i, status, output_path))
        print(f"\nRun {i}: {status} -> {output_path}")

        # Fully close VS Code before the next run
        if i < args.n:
            close_vscode()

    # Print summary
    print(f"\n{'='*60}")
    print("  RESULTS SUMMARY")
    print(f"{'='*60}")
    for run_num, status, path in results:
        print(f"  Run {run_num}: {status}  ->  {path}")

    passed = sum(1 for _, s, _ in results if s == "PASS")
    print(f"\n  {passed}/{args.n} passed")
    print(f"{'='*60}\n")

    # Combine all recordings into a grid video
    video_paths = [path for _, _, path in results if os.path.exists(path)]
    if len(video_paths) > 1:
        grid_path = os.path.join(args.output_dir, "grid_all_runs.mp4")
        create_grid_video(video_paths, grid_path)


if __name__ == "__main__":
    main()
