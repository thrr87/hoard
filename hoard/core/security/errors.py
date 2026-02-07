from __future__ import annotations

from hoard.core.errors import HoardError


class AuthError(HoardError):
    pass


class ScopeError(HoardError):
    pass
