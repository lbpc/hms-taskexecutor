import sys
from unittest.mock import MagicMock

sys.modules['taskexecutor.config'] = MagicMock()

from taskexecutor.config import CONFIG
