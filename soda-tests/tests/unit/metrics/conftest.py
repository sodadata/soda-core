import sys
from pathlib import Path

# Add unit directory to path so we can import from helpers
unit_dir = Path(__file__).parent.parent
if str(unit_dir) not in sys.path:
    sys.path.insert(0, str(unit_dir))
