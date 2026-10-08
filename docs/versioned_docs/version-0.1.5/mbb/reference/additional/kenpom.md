---
title: "MBB — additional Python functions — KenPom"
sidebar_label: "KenPom"
sidebar_position: 6
description: "MBB — additional Python functions — KenPom — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — KenPom

### has_kenpom_login {#has_kenpom_login}

`has_kenpom_login() -> 'bool'`

Whether KenPom credentials are set in the environment.

The Python counterpart of hoopR's `has_kp_user_and_pw()`; gates a live
test without attempting a login.

**Returns**

`True` when both an e-mail and a password resolve from the environment.

**Example**

```python
import pytest
from sportsdataverse.mbb import has_kenpom_login

pytestmark = pytest.mark.skipif(not has_kenpom_login(), reason="no KenPom login")
```

### kenpom_login {#kenpom_login}

`kenpom_login(email: 'Optional[str]' = None, password: 'Optional[str]' = None, *, proxy: 'Any' = None) -> 'requests.Session'`

Log into kenpom.com and return the authenticated session.

The Python counterpart of hoopR's `login()`. Calling this directly is
optional -- every wrapper logs in on demand and reuses a cached session --
but it is the fastest way to verify credentials or a proxy before a long
pull, and the returned session can be passed to a wrapper as `session=`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `email` | `Optional[str]` | `None` | KenPom account e-mail. Falls back to `KENPOM_EMAIL` / `KP_USER` / `SDV_PY_KENPOM_EMAIL`. |
| `password` | `Optional[str]` | `None` | KenPom password. Falls back to `KENPOM_PW` / `KP_PW` / `SDV_PY_KENPOM_PW`. |
| `proxy` | `Any` | `None` | Proxy URL `str` or `requests` `proxies=` `dict`. Falls back to `SDV_PY_KENPOM_PROXY` then `SDV_PY_PROXY`. |

**Returns**

An authenticated `requests.Session` carrying the subscription cookie and the resolved proxy.

**Example**

```python
from sportsdataverse.mbb import kenpom_login

session = kenpom_login(proxy="http://user:pw@proxy.example:8080")
```
