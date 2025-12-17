from typing import Dict, Any

from .client import geocode_city, GoogleApiError


def google_health_probe() -> Dict[str, Any]:
    """
    Minimal health probe for Google Maps Platform.

    Returns:
        dict: {"google_maps": "ok"} on success, or an error description.
    """
    try:
        geocode_city("Toronto", "CA")
        return {"google_maps": "ok"}
    except GoogleApiError as e:
        return {"google_maps": f"error: {e}"}
    except Exception as e:
        return {"google_maps": f"unexpected_error: {type(e).__name__}: {e}"}
