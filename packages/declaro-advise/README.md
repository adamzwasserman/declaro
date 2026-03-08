# declaro-advise

Notification and messaging system for the declaro stack.

## Overview

declaro-advise provides notification functions compatible with the buckler notification system. When deployed back into buckler/idd, these will be replaced with the full implementation.

For standalone use, notifications are logged but not displayed.

## Installation

```bash
pip install declaro-advise
```

## Usage

```python
from declaro_advise import success, error, info, warning

# Send notifications
success("Operation completed successfully")
error("Something went wrong")
info("Here's some information")
warning("Be careful about this")
```

## Functions

- `success(message, duration=3000, **kwargs)` - Green checkmark notification
- `error(message, priority=Priority.INFORMATIONAL, **kwargs)` - Red X notification
- `info(message, action_url=None, action_label=None, **kwargs)` - Blue info notification
- `warning(message, priority=Priority.INFORMATIONAL, **kwargs)` - Yellow warning notification

## License

MIT
