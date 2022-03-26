from __future__ import annotations

import datetime


def now():
    return datetime.datetime.now().isoformat()
