from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional, List

# Suppress noisy logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import soda_logger

SYSTEM_PROMPT_FILE = Path(__file__).parent / "code_system_prompt.txt"

SUGGESTED_PROMPTS = [
    ("1", "Translate ODCS contract to Soda Contract", "I have an ODCS contract that I'd like to translate to Soda Contract Language. Here's my ODCS contract:\n\n```yaml\n# Paste your ODCS contract here\n```"),
    ("2", "Translate Soda v3 checks to v4 contract", "I have a Soda v3 checks file that I'd like to translate to a v4 data contract. Here's my v3 checks file:\n\n```yaml\n# Paste your v3 checks here\n```"),
    ("3", "Create a new data contract", "Help me create a new Soda data contract for a table with the following columns: ..."),
]

# OpenAI function calling tool definitions
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read the contents of a file from the user's filesystem. Use this when the user references a file that needs to be read (e.g., a YAML contract, checks file, SQL file).",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file to read (relative to the current working directory or absolute).",
                    }
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write content to a file on the user's filesystem. Use this to save translated contracts, generated YAML, or any output the user needs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file to write (relative to the current working directory or absolute).",
                    },
                    "content": {
                        "type": "string",
                        "description": "The content to write to the file.",
                    },
                },
                "required": ["file_path", "content"],
            },
        },
    },
]

# --- ANSI colors ---
DIM = "\033[2m"
RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"


class Spinner:
    """A simple terminal spinner that runs in a background thread."""

    FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, message: str = "Thinking"):
        self._message = message
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join()
        # Clear the spinner line
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

    def update(self, message: str) -> None:
        self._message = message

    def _spin(self) -> None:
        i = 0
        while not self._stop_event.is_set():
            frame = self.FRAMES[i % len(self.FRAMES)]
            sys.stdout.write(f"\r{DIM}{frame} {self._message}...{RESET}")
            sys.stdout.flush()
            i += 1
            time.sleep(0.08)


def handle_code_chat(verbose: bool = False) -> ExitCode:
    """Handle the soda code chat command."""
    try:
        from openai import OpenAI, APIError
    except ImportError:
        soda_logger.error(
            "The 'openai' package is required for soda code. "
            "Install it with: pip install openai"
        )
        return ExitCode.LOG_ERRORS

    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.formatted_text import ANSI as ANSI_Text
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.keys import Keys
    except ImportError:
        soda_logger.error(
            "The 'prompt_toolkit' package is required for soda code. "
            "Install it with: pip install prompt_toolkit"
        )
        return ExitCode.LOG_ERRORS

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        soda_logger.error(
            "OPENAI_API_KEY environment variable is not set. "
            "Please set it with your OpenAI API key."
        )
        return ExitCode.LOG_ERRORS

    email = _prompt_for_email()
    if email is None:
        return ExitCode.LOG_ERRORS

    print(f"\n{CYAN}{'=' * 60}{RESET}")
    print(f"{BOLD}Welcome to Soda Code!{RESET}")
    print(f"{CYAN}{'=' * 60}{RESET}")
    print(f"\nI can help you with:")
    print(f"  {DIM}-{RESET} Translating ODCS contracts to Soda Contract Language")
    print(f"  {DIM}-{RESET} Translating Soda v3 checks to v4 data contracts")
    print(f"  {DIM}-{RESET} Writing and understanding data contracts")
    print(f"\n{BOLD}Suggested prompts:{RESET}")
    for num, title, _ in SUGGESTED_PROMPTS:
        print(f"  {CYAN}[{num}]{RESET} {title}")
    print(f"\n{DIM}Just mention files naturally (e.g., 'translate my_checks.yml to v4'){RESET}")
    print(f"{DIM}Files are read and results are saved automatically{RESET}")
    print(f"{DIM}Enter to submit | Shift+Enter or Alt+Enter for new line | 'exit' to quit{RESET}")
    print(f"{CYAN}{'=' * 60}{RESET}\n")

    # Set up key bindings for multi-line input
    from prompt_toolkit.input.ansi_escape_sequences import ANSI_SEQUENCES

    ANSI_SEQUENCES['\x1b[13;2u'] = Keys.F24
    ANSI_SEQUENCES['\x1b[27;2;13~'] = Keys.F24

    bindings = KeyBindings()

    @bindings.add(Keys.Enter)
    def _(event):
        event.current_buffer.validate_and_handle()

    @bindings.add(Keys.F24)
    def _(event):
        event.current_buffer.insert_text('\n')

    @bindings.add(Keys.Escape, Keys.Enter)
    def _(event):
        event.current_buffer.insert_text('\n')

    session = PromptSession(key_bindings=bindings, multiline=True)
    client = OpenAI(api_key=api_key)
    system_prompt = SYSTEM_PROMPT_FILE.read_text(encoding="utf-8")
    messages = [{"role": "system", "content": system_prompt}]

    while True:
        try:
            user_input = session.prompt(ANSI_Text(f"\n{BOLD}You:{RESET} ")).strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n{DIM}Goodbye!{RESET}")
            break

        if not user_input:
            continue

        if user_input.lower() in ("exit", "quit", "q"):
            print(f"\n{DIM}Goodbye!{RESET}")
            break

        # Check if user selected a suggested prompt
        for num, title, prompt_template in SUGGESTED_PROMPTS:
            if user_input == num:
                print(f"\n{CYAN}[Using suggested prompt: {title}]{RESET}")
                print("Enter your content (Enter to submit):")
                try:
                    additional_input = session.prompt("").strip()
                    if additional_input:
                        user_input = additional_input
                    else:
                        user_input = prompt_template
                except (EOFError, KeyboardInterrupt):
                    user_input = prompt_template
                break

        messages.append({"role": "user", "content": user_input})

        try:
            response_text = _run_agent_loop(client, messages)
            if response_text:
                messages.append({"role": "assistant", "content": response_text})
        except APIError as e:
            soda_logger.error(f"API error: {e}")
            messages.pop()
            continue

    return ExitCode.OK


def _run_agent_loop(client, messages: List[dict]) -> Optional[str]:
    """Run the agent loop: call the LLM, execute tool calls, repeat until done."""
    spinner = Spinner("Thinking")
    spinner.start()
    did_tool_call = False

    while True:
        response = client.chat.completions.create(
            model="gpt-4o",
            max_tokens=4096,
            messages=messages,
            tools=TOOLS,
        )

        choice = response.choices[0]

        if choice.finish_reason == "tool_calls":
            messages.append(choice.message.to_dict())

            for tool_call in choice.message.tool_calls:
                tool_name = tool_call.function.name
                status, done_msg = _tool_status(tool_name, tool_call.function.arguments)

                spinner.update(status)
                result = _execute_tool(tool_name, tool_call.function.arguments)

                # Print a log line showing the completed action
                spinner.stop()
                print(f"  {DIM}{done_msg}{RESET}")
                spinner.start()

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result,
                })
                did_tool_call = True

            spinner.update("Generating" if did_tool_call else "Thinking")
            continue

        spinner.stop()

        text = choice.message.content or ""
        print(f"\n{BOLD}Soda Code:{RESET} {text}")
        return text


def _tool_status(name: str, arguments: str) -> tuple:
    """Build spinner text and a done-log message for a tool call.

    Returns:
        (spinner_text, done_message)
    """
    try:
        args = json.loads(arguments)
    except json.JSONDecodeError:
        return name, name

    if name == "read_file":
        fp = args.get("file_path", "file")
        return f"Reading {fp}", f"Read {fp}"
    elif name == "write_file":
        fp = args.get("file_path", "file")
        return f"Saving {fp}", f"Saved {fp}"
    return name, name


def _execute_tool(name: str, arguments: str) -> str:
    """Execute a tool call and return the result as a string."""
    try:
        args = json.loads(arguments)
    except json.JSONDecodeError:
        return f"Error: Invalid arguments: {arguments}"

    if name == "read_file":
        return _tool_read_file(args.get("file_path", ""))
    elif name == "write_file":
        return _tool_write_file(args.get("file_path", ""), args.get("content", ""))
    else:
        return f"Error: Unknown tool '{name}'"


def _tool_read_file(file_path: str) -> str:
    """Read a file and return its contents."""
    path = Path(file_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    if not path.exists():
        return f"File not found: {file_path}"
    if not path.is_file():
        return f"Not a file: {file_path}"

    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        return f"Error reading {file_path}: {e}"


def _tool_write_file(file_path: str, content: str) -> str:
    """Write content to a file."""
    path = Path(file_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return f"Successfully saved to {file_path}"
    except Exception as e:
        return f"Error writing {file_path}: {e}"


def _prompt_for_email() -> Optional[str]:
    """Prompt the user for their work email."""
    print(f"\n{CYAN}{'=' * 60}{RESET}")
    print(f"{BOLD}Soda Code{RESET} {DIM}- AI Assistant for Data Contracts{RESET}")
    print(f"{CYAN}{'=' * 60}{RESET}")
    print(f"\nPlease enter your work email to continue:")

    try:
        email = input(f"{BOLD}Email:{RESET} ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n")
        return None

    if not email:
        soda_logger.error("Email is required to use Soda Code.")
        return None

    if "@" not in email or "." not in email:
        soda_logger.error("Please enter a valid email address.")
        return None

    # TODO: Add validation for personal email domains (gmail, outlook, hotmail, etc.)
    # TODO: Save email to database

    print(f"\n{GREEN}Welcome, {email}!{RESET}")
    return email
