from __future__ import annotations

from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry


def build_session() -> Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_json(session: Session, url: str, params: dict[str, object], timeout: int, service_name: str):
    try:
        response: Response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except RequestException as exc:
        raise RuntimeError(
            f"Could not reach {service_name}. This is usually a temporary network or DNS issue. "
            f"Please try again in a moment."
        ) from exc
