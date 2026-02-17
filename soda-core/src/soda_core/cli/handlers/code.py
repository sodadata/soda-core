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

PROMPTS_DIR = Path(__file__).parent / "prompts"

INTENT_PROMPT_FILES = {
    "v3_to_v4": PROMPTS_DIR / "v3_to_v4.txt",
    "odcs_to_soda": PROMPTS_DIR / "odcs_to_soda.txt",
    "general": PROMPTS_DIR / "general.txt",
}

ROUTER_PROMPT_FILE = PROMPTS_DIR / "router.txt"

# Cache for loaded prompt texts (populated on first access)
_prompt_cache: dict[str, str] = {}

SUGGESTED_PROMPTS = [
    ("1", "Translate ODCS contract to Soda Data Contract", "I have an ODCS contract that I'd like to translate to Soda Contract Language. Here's my ODCS contract:\n\n```yaml\n# Paste your ODCS contract here\n```"),
    ("2", "Translate Soda v3 checks file to a Soda Data Contract (v4)", "I have a Soda v3 checks file that I'd like to translate to a v4 data contract. Here's my v3 checks file:\n\n```yaml\n# Paste your v3 checks here\n```"),
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
    {
        "type": "function",
        "function": {
            "name": "find_file",
            "description": "Search for a file by name in the current directory and subdirectories. Use this when read_file fails because the file was not found, or when the user mentions a filename without a full path and you're unsure where it lives.",
            "parameters": {
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "The filename or partial filename to search for (e.g., 'v3_checks.yml', 'checks').",
                    }
                },
                "required": ["filename"],
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

# --- Email config ---
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com",
    "yahoo.com", "yahoo.co.uk", "icloud.com", "me.com", "mac.com",
    "protonmail.com", "proton.me", "aol.com", "mail.com", "zoho.com",
    "yandex.com", "gmx.com", "gmx.net",
}

SODA_CONFIG_DIR = Path.home() / ".soda"
SODA_CODE_CONFIG = SODA_CONFIG_DIR / "code_config.json"

# --- Slash commands ---
SLASH_COMMANDS = {
    "/help": "Show available commands and usage tips",
    "/clear": "Clear conversation history and start fresh",
    "/history": "Show recent conversation messages",
    "/files": "List relevant files in the current directory",
    "/model": "Show current model and routing info",
    "/exit": "Exit the chat",
}

FILE_EXTENSIONS = {".yml", ".yaml", ".sql"}


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


def _load_prompt(intent: str) -> str:
    """Load and cache a prompt file for the given intent."""
    if intent not in _prompt_cache:
        path = INTENT_PROMPT_FILES.get(intent, INTENT_PROMPT_FILES["general"])
        _prompt_cache[intent] = path.read_text(encoding="utf-8")
    return _prompt_cache[intent]


def _get_conversation_summary(messages: List[dict]) -> str:
    """Extract a short summary of recent conversation for the router.

    Returns the last ~4 user/assistant messages, truncated to keep the
    router call small and fast.
    """
    recent = []
    for msg in reversed(messages):
        if msg["role"] in ("user", "assistant") and msg.get("content"):
            text = msg["content"][:200]
            recent.append(f"{msg['role']}: {text}")
            if len(recent) >= 4:
                break
    recent.reverse()
    return "\n".join(recent)


def _classify_intent(client, user_message: str, messages: List[dict]) -> str:
    """Classify user intent using a fast gpt-4o-mini call.

    Returns one of: 'v3_to_v4', 'odcs_to_soda', 'general'.
    Falls back to 'general' on any error.
    """
    try:
        router_prompt = _prompt_cache.get("_router")
        if not router_prompt:
            router_prompt = ROUTER_PROMPT_FILE.read_text(encoding="utf-8")
            _prompt_cache["_router"] = router_prompt

        conversation_context = _get_conversation_summary(messages)
        user_content = f"Conversation context:\n{conversation_context}\n\nNew message to classify:\n{user_message}"

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=20,
            temperature=0,
            messages=[
                {"role": "system", "content": router_prompt},
                {"role": "user", "content": user_content},
            ],
        )

        intent = response.choices[0].message.content.strip().lower()
        if intent in INTENT_PROMPT_FILES:
            return intent
        return "general"
    except Exception:
        return "general"


# --- Email gate ---


def _load_email_from_config() -> Optional[str]:
    """Load email from ~/.soda/code_config.json."""
    try:
        if SODA_CODE_CONFIG.exists():
            config = json.loads(SODA_CODE_CONFIG.read_text(encoding="utf-8"))
            return config.get("email")
    except (json.JSONDecodeError, OSError):
        pass
    return None


def _save_email_to_config(email: str) -> None:
    """Save email to ~/.soda/code_config.json, preserving existing keys."""
    try:
        SODA_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        config = {}
        if SODA_CODE_CONFIG.exists():
            try:
                config = json.loads(SODA_CODE_CONFIG.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        config["email"] = email
        SODA_CODE_CONFIG.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def _load_or_prompt_email() -> tuple[Optional[str], bool]:
    """Load email from config or prompt user. Returns (email, is_new_user)."""
    saved = _load_email_from_config()
    if saved:
        return saved, False

    print(f"\n{BOLD}Soda Code{RESET} {DIM}- AI Assistant for Data Contracts{RESET}")
    print(f"\nPlease enter your work email to continue:")

    while True:
        try:
            email = input(f"{BOLD}Email:{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n")
            return None, False

        if not email:
            print(f"{RED}Email is required to use Soda Code.{RESET}")
            continue

        if "@" not in email or "." not in email.split("@")[-1]:
            print(f"{RED}Please enter a valid email address.{RESET}")
            continue

        domain = email.split("@")[-1].lower()
        if domain in PERSONAL_EMAIL_DOMAINS:
            print(f"{RED}Please use a work email address (not {domain}).{RESET}")
            continue

        _save_email_to_config(email)
        return email, True


# --- File discovery ---


def _find_relevant_files() -> List[Path]:
    """Find .yml, .yaml, .sql, .json files in CWD recursively."""
    cwd = Path.cwd()
    return sorted(
        [p for p in cwd.rglob("*")
         if p.is_file() and p.suffix in FILE_EXTENSIONS and not p.name.startswith(".")],
        key=lambda p: p.name,
    )


# --- Slash commands ---


def _cmd_help() -> None:
    """Show available commands and usage tips."""
    print(f"\n{BOLD}Available commands:{RESET}")
    for cmd, desc in SLASH_COMMANDS.items():
        print(f"  {CYAN}{cmd:<12}{RESET} {desc}")
    print(f"\n{BOLD}Tips:{RESET}")
    print(f"  {DIM}-{RESET} Mention file names naturally and they'll be read automatically")
    print(f"  {DIM}-{RESET} Translated contracts are saved with your confirmation")
    print(f"  {DIM}-{RESET} Use {CYAN}[1]{RESET} {CYAN}[2]{RESET} {CYAN}[3]{RESET} for suggested prompts")
    print(f"  {DIM}-{RESET} Tab to autocomplete file paths and commands")


def _cmd_clear(messages: List[dict]) -> bool:
    """Clear conversation history. Returns True if confirmed."""
    if len(messages) <= 1:
        print(f"{DIM}Nothing to clear.{RESET}")
        return False
    try:
        answer = input(f"  {YELLOW}Clear conversation history? [y/N]{RESET} ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    if answer in ("y", "yes"):
        messages.clear()
        messages.append({"role": "system", "content": _load_prompt("general")})
        print(f"{GREEN}Conversation cleared.{RESET}")
        return True
    return False


def _cmd_history(messages: List[dict]) -> None:
    """Show last 10 user/assistant messages."""
    relevant = [m for m in messages if m["role"] in ("user", "assistant") and m.get("content")]
    if not relevant:
        print(f"{DIM}No messages yet.{RESET}")
        return
    last_10 = relevant[-10:]
    print(f"\n{BOLD}Recent messages:{RESET}")
    for msg in last_10:
        role = "You" if msg["role"] == "user" else "Assistant"
        text = msg["content"][:120]
        if len(msg["content"]) > 120:
            text += "..."
        color = CYAN if msg["role"] == "user" else ""
        reset_color = RESET if color else ""
        print(f"  {BOLD}{role}:{RESET} {color}{text}{reset_color}")


def _cmd_files() -> None:
    """List relevant files in CWD."""
    all_files = _find_relevant_files()
    if not all_files:
        print(f"{DIM}No .yml, .yaml, or .sql files found.{RESET}")
        return
    total = len(all_files)
    shown = all_files[:30]
    cwd = Path.cwd()
    print(f"\n{BOLD}Files in {cwd}:{RESET}")
    for f in shown:
        rel = f.relative_to(cwd)
        print(f"  {DIM}-{RESET} {rel}")
    if total > 30:
        print(f"  {DIM}... and {total - 30} more{RESET}")


def _cmd_model(current_intent: str) -> None:
    """Show current model and routing info."""
    print(f"\n{BOLD}Model info:{RESET}")
    print(f"  {DIM}Chat model:{RESET}   gpt-4o")
    print(f"  {DIM}Router model:{RESET} gpt-4o-mini")
    print(f"  {DIM}Active intent:{RESET} {current_intent}")
    prompt_file = INTENT_PROMPT_FILES.get(current_intent, INTENT_PROMPT_FILES["general"])
    print(f"  {DIM}Prompt file:{RESET}  {prompt_file.name}")


def _handle_slash_command(command: str, messages: List[dict], current_intent: str) -> str:
    """Dispatch a slash command. Returns 'handled', 'exit', or 'clear'."""
    cmd = command.strip().split()[0].lower()

    if cmd == "/help":
        _cmd_help()
        return "handled"
    elif cmd == "/clear":
        cleared = _cmd_clear(messages)
        return "clear" if cleared else "handled"
    elif cmd == "/history":
        _cmd_history(messages)
        return "handled"
    elif cmd == "/files":
        _cmd_files()
        return "handled"
    elif cmd == "/model":
        _cmd_model(current_intent)
        return "handled"
    elif cmd == "/exit":
        return "exit"
    else:
        print(f"{DIM}Unknown command. Type /help to see available commands.{RESET}")
        return "handled"


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
        from prompt_toolkit.completion import Completer, Completion, PathCompleter
        from prompt_toolkit.document import Document
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

    # --- Email gate ---
    email, is_new_user = _load_or_prompt_email()
    if email is None:
        return ExitCode.LOG_ERRORS

    # --- File autocomplete ---
    class SodaCompleter(Completer):
        def __init__(self):
            self._path_completer = PathCompleter(
                file_filter=lambda name: any(name.endswith(ext) for ext in FILE_EXTENSIONS),
                expanduser=True,
            )

        def get_completions(self, document, complete_event):
            text = document.text_before_cursor

            # Slash command completion
            if text.lstrip().startswith("/"):
                word = text.lstrip()
                for cmd in SLASH_COMMANDS:
                    if cmd.startswith(word) and cmd != word:
                        yield Completion(cmd, start_position=-len(word))
                return

            # File path completion — detect path-like tokens
            words = text.split()
            if not words:
                return
            current_word = words[-1]

            is_path_like = (
                current_word.startswith("./")
                or current_word.startswith("../")
                or current_word.startswith("/")
                or current_word.startswith("~/")
                or "/" in current_word
                or any(current_word.endswith(ext) for ext in FILE_EXTENSIONS)
            )

            if is_path_like:
                sub_doc = Document(current_word, len(current_word))
                yield from self._path_completer.get_completions(sub_doc, complete_event)

    # --- Onboarding ---
    found_files = _find_relevant_files()

    print(f"\n{CYAN}{'=' * 60}{RESET}")
    print(f"{BOLD}Soda Code{RESET} {DIM}- AI Assistant for Data Contracts{RESET}")
    print(f"{CYAN}{'=' * 60}{RESET}")

    if is_new_user:
        print(f"\n{GREEN}Welcome, {email}!{RESET}")
        print(f"\n  I can help you with:")
        print(f"  {DIM}-{RESET} Translating ODCS contracts to Soda Contract Language")
        print(f"  {DIM}-{RESET} Translating Soda v3 checks to v4 data contracts")
        print(f"  {DIM}-{RESET} Writing and understanding data contracts")
    else:
        print(f"\n{GREEN}Welcome back, {email}!{RESET}")

    if found_files:
        yaml_count = sum(1 for f in found_files if f.suffix in (".yml", ".yaml"))
        sql_count = sum(1 for f in found_files if f.suffix == ".sql")
        parts = []
        if yaml_count:
            parts.append(f"{yaml_count} YAML")
        if sql_count:
            parts.append(f"{sql_count} SQL")
        hint = " and ".join(parts)
        sample = next((f for f in found_files if f.suffix in (".yml", ".yaml")), found_files[0])
        sample_name = sample.relative_to(Path.cwd())
        print(f"\n  {DIM}Found {hint} file{'s' if len(found_files) > 1 else ''} — try: 'translate {sample_name} to v4'{RESET}")

    print(f"\n{BOLD}Suggested prompts:{RESET}")
    for num, title, _ in SUGGESTED_PROMPTS:
        print(f"  {CYAN}[{num}]{RESET} {title}")

    print(f"\n{DIM}Enter to submit | Shift+Enter for new line | /help for commands{RESET}")
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

    session = PromptSession(
        key_bindings=bindings,
        multiline=True,
        completer=SodaCompleter(),
        complete_while_typing=False,
    )
    client = OpenAI(api_key=api_key)
    current_intent = "general"
    messages = [{"role": "system", "content": _load_prompt(current_intent)}]

    while True:
        try:
            user_input = session.prompt(ANSI_Text(f"\n{CYAN}{BOLD}❯{RESET} ")).strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n{DIM}Goodbye!{RESET}")
            break

        if not user_input:
            continue

        if user_input.lower() in ("exit", "quit", "q"):
            print(f"\n{DIM}Goodbye!{RESET}")
            break

        # Slash commands
        if user_input.startswith("/"):
            result = _handle_slash_command(user_input, messages, current_intent)
            if result == "exit":
                print(f"\n{DIM}Goodbye!{RESET}")
                break
            if result == "clear":
                current_intent = "general"
            continue

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

        # Route intent and swap system prompt if needed
        intent = _classify_intent(client, user_input, messages)
        if intent != current_intent:
            current_intent = intent
            messages[0] = {"role": "system", "content": _load_prompt(current_intent)}

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

                # Stop spinner before write so the confirmation prompt renders cleanly
                if tool_name == "write_file":
                    spinner.stop()

                result = _execute_tool(tool_name, tool_call.function.arguments)

                # Print a log line showing the completed action
                if tool_name != "write_file":
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
        print(f"\n{BOLD}⏺{RESET} {text}")
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
    elif name == "find_file":
        fn = args.get("filename", "file")
        return f"Searching for {fn}", f"Searched for {fn}"
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
    elif name == "find_file":
        return _tool_find_file(args.get("filename", ""))
    else:
        return f"Error: Unknown tool '{name}'"


def _tool_read_file(file_path: str) -> str:
    """Read a file and return its contents."""
    path = Path(file_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    if not path.exists():
        return f"File not found: {file_path}. Try using find_file to search for it."
    if not path.is_file():
        return f"Not a file: {file_path}"

    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        return f"Error reading {file_path}: {e}"


def _tool_write_file(file_path: str, content: str) -> str:
    """Write content to a file, after user confirmation."""
    path = Path(file_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    exists = path.exists()
    action = "Overwrite" if exists else "Create"

    try:
        answer = input(f"  {YELLOW}{action} {file_path}? [Y/n]{RESET} ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return f"User cancelled saving to {file_path}"

    if answer and answer not in ("y", "yes"):
        return f"User declined saving to {file_path}"

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return f"Successfully saved to {file_path}"
    except Exception as e:
        return f"Error writing {file_path}: {e}"


def _tool_find_file(filename: str) -> str:
    """Search for a file by name in CWD recursively."""
    if not filename:
        return "Error: filename is required."

    cwd = Path.cwd()
    skip_dirs = {".git", "node_modules", "__pycache__", ".venv", "venv", ".tox",
                 ".eggs", ".mypy_cache", ".pytest_cache", "dist", "build", ".cache"}

    exact = []
    partial = []

    for p in cwd.rglob("*"):
        # Skip noisy directories
        try:
            rel = p.relative_to(cwd)
        except ValueError:
            continue
        if any(part in skip_dirs for part in rel.parts):
            continue
        if not p.is_file():
            continue

        if p.name == filename:
            exact.append(rel)
        elif filename.lower() in p.name.lower():
            partial.append(rel)

        # Stop scanning after enough results
        if len(exact) + len(partial) >= 20:
            break

    matches = exact or partial
    if not matches:
        return f"No files matching '{filename}' found."

    label = "exact" if exact else "partial"
    lines = [f"Found {len(matches)} {label} match{'es' if len(matches) != 1 else ''}:"]
    for i, rel in enumerate(matches[:10], 1):
        lines.append(f"  {i}. {rel}")
    if len(matches) > 10:
        lines.append(f"  ... and {len(matches) - 10} more")

    return "\n".join(lines)
