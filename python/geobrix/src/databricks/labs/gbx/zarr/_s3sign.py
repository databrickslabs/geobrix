"""AWS Signature Version 4 for a body-less S3 request, with the standard library only.

obstore cannot abort the multipart upload of a pack it failed to put (it sends no
``AbortMultipartUpload``), so ``_access`` lists the incomplete uploads of the key and aborts them
itself; those two requests need signing. Only ``GET`` and ``DELETE`` with no body are needed here.
"""

from __future__ import annotations

import hashlib
import hmac
from urllib.parse import quote

__all__ = ["sign_v4"]

_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _hmac(key: bytes, text: str) -> bytes:
    return hmac.new(key, text.encode(), hashlib.sha256).digest()


def sign_v4(
    method: str,
    host: str,
    path: str,
    query: list[tuple[str, str]],
    *,
    access_key: str,
    secret_key: str,
    session_token: str | None,
    region: str,
    amz_date: str,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, str]:
    """The headers (``Authorization`` and the ones it signs) of an S3 request with no body.

    ``path`` is the unencoded request path, ``query`` its parameters as (name, value) pairs
    (an empty value for ``?uploads``), ``amz_date`` ``YYYYMMDDTHHMMSSZ``.
    """
    headers = {
        "host": host,
        "x-amz-content-sha256": _EMPTY_SHA256,
        "x-amz-date": amz_date,
    }
    if session_token:
        headers["x-amz-security-token"] = session_token
    headers.update({k.lower(): v.strip() for k, v in (extra_headers or {}).items()})
    names = sorted(headers)
    canonical = "\n".join(
        [
            method,
            quote(path, safe="/-_.~"),  # S3 does not encode the path twice
            "&".join(
                f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}"
                for k, v in sorted(query)
            ),
            "".join(f"{n}:{headers[n]}\n" for n in names),
            ";".join(names),
            _EMPTY_SHA256,
        ]
    )
    day = amz_date[:8]
    scope = f"{day}/{region}/s3/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical.encode()).hexdigest(),
        ]
    )
    key = _hmac(("AWS4" + secret_key).encode(), day)
    for part in (region, "s3", "aws4_request"):
        key = _hmac(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={';'.join(names)}, Signature={signature}"
    )
    return headers
