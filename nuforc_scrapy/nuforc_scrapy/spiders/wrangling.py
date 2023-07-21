def pick_first(iterable):
    """
    Custom output processor to always pick the first item from the iterable.
    If the iterable is empty, it returns None.
    """
    if iterable:
        return next(iter(iterable))
    return None
