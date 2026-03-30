"""Automate VS Code Jupyter Notebook creation with screen recording."""

import argparse
import os
import shutil
import subprocess
import sys
import time

import pyautogui
import pyperclip

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
sampled_df.show()'''


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


def automate_vscode():
    pyautogui.PAUSE = 0.3
    pyautogui.FAILSAFE = True

    # Step 1: Open VS Code
    print("Opening VS Code...")
    subprocess.Popen("code", shell=True)
    time.sleep(5)

    # Step 2: Open Command Palette and create new Jupyter Notebook
    print("Creating new Jupyter Notebook...")
    pyautogui.hotkey("ctrl", "shift", "p")
    time.sleep(1)

    pyautogui.write("Create: New Jupyter Notebook", interval=0.04)
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

    # Step 4: Click "Select Kernel" via Command Palette
    print("Opening kernel picker...")
    pyautogui.hotkey("ctrl", "shift", "p")
    time.sleep(1)
    pyautogui.write("Notebook: Select Notebook Kernel", interval=0.04)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 5: Click "Select Another Kernel"
    print("Selecting 'Select Another Kernel'...")
    pyautogui.write("Select Another Kernel", interval=0.04)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 6: Click "Remote Spark Kernel"
    print("Selecting 'Remote Spark Kernel'...")
    pyautogui.write("Remote Spark Kernel", interval=0.04)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(2)

    # Step 7: Select the first one in the list (might take a long time)
    print("Selecting first kernel in list (this may take a while)...")
    time.sleep(3)
    pyautogui.press("enter")
    time.sleep(30)  # kernel connection can be very slow

    # Step 8: Execute all cells
    print("Executing all cells...")
    pyautogui.hotkey("ctrl", "shift", "p")
    time.sleep(1)
    pyautogui.write("Notebook: Execute All Cells", interval=0.04)
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(10)

    print("Automation complete.")


def main():
    parser = argparse.ArgumentParser(description="Automate VS Code Jupyter Notebook creation with screen recording")
    parser.add_argument("--output", default="output/recording.mp4", help="Output MP4 file path (default: output/recording.mp4)")
    args = parser.parse_args()

    validate_dependencies()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    ffmpeg_proc = start_recording(args.output)
    try:
        automate_vscode()
        time.sleep(2)
    finally:
        stop_recording(ffmpeg_proc)

    print(f"Recording saved to: {args.output}")


if __name__ == "__main__":
    main()
