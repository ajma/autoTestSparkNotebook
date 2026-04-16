# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Cross-platform (Windows, macOS, Linux) automation tool that tests PySpark notebook execution via the Data Cloud Extension in VS Code and Antigravity (a VS Code fork). It launches the IDE, creates a Jupyter notebook, connects to a remote Spark kernel provided by Data Cloud Extension, runs PySpark code, and records the screen -- all unattended.

## Commands

```bash
# Run automation (VS Code, single run)
python automate.py run

# Run with Antigravity, 9 iterations
python automate.py run --app antigravity -n 9

# Loop forever (grid video every 9 runs)
python automate.py run --app antigravity --loop

# Google Sheets OAuth login
python automate.py login

# Install dependencies
pip install -r requirements.txt
pip install playwright gspread google-auth google-auth-oauthlib google-genai
playwright install chromium
```

There are no tests or linting configured for this project.

## Architecture

The entire codebase is a single file: `automate.py`. Key sections:

- **CLI entry point** (`main`): argparse with `run` and `login` subcommands. `run` is the default.
- **IDE automation** (`automate_vscode`): Drives VS Code/Antigravity via Playwright CDP -- creates notebook, pastes code, selects remote Spark kernel, runs cells. Uses `pyautogui` only for native OS dialogs (Save As).
- **Command palette helpers** (`run_command_palette`, `select_from_quick_pick`): Interact with VS Code's quick-input UI via Playwright DOM selectors.
- **Cell polling** (`wait_for_cell_done`, `check_notebook_output`): Periodically saves the notebook (Ctrl+S) and reads the `.ipynb` JSON from disk, looking for `EXECUTION_COMPLETE_MARKER` or error outputs.
- **Screen recording** (`start_recording`, `stop_recording`): ffmpeg capture (`gdigrab` on Windows, `avfoundation` on macOS, `x11grab` on Linux). Only failed-run recordings are kept.
- **Grid video** (`create_grid_video`): Combines multiple recordings into an NxN grid via ffmpeg `xstack`.
- **Google Sheets** (`append_to_google_sheet`, `gsheets_login`): OAuth2 token flow, appends result rows.
- **Failure analysis** (`analyze_log_with_gemini`): Sends Jupyter server logs to Gemini 2.5 Flash for a 150-char failure summary.
- **Sleep prevention** (`prevent_sleep`, `allow_sleep`): `SetThreadExecutionState` on Windows, `caffeinate` on macOS, `xset` on Linux.

## Key Design Details

- **Cross-platform**: Platform-specific implementations for ESC key listener (`msvcrt` on Windows, `termios`/`select` on Unix), sleep prevention, screen capture, process management (`taskkill` on Windows, `pkill` on macOS/Linux), and keyboard modifiers (`Ctrl` on Windows/Linux, `Cmd` on macOS).
- **CDP connection**: Launches IDE with `--remote-debugging-port=9222`, connects via `playwright.chromium.connect_over_cdp()`.
- **Notebook save path**: Notebooks are saved to `~/spark_test_notebooks/spark_test_{N}.ipynb` so the script can read output from disk.
- **Config**: `config.json` (gitignored) holds `sheet_id` and `gemini_api_key`. See `config.example.json` for structure.
- **Credentials**: `credentials.json` (OAuth client) and `token.json` (cached token) are both gitignored.
- **ESC to stop**: A daemon thread polls for ESC key press to gracefully stop between runs.
