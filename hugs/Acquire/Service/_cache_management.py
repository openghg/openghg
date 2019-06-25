
__all__ = ["clear_service_cache"]


def clear_service_cache():
    """Call this function to clear the cache of the current service.
       This is only really needed for testhing, when a single python
      interpreter will move between multiple (cached) services
    """

    from Acquire.Service import clear_services_cache
    from Acquire.Service import clear_serviceinfo_cache, clear_login_cache

    clear_services_cache()
    clear_login_cache()
    clear_serviceinfo_cache()
