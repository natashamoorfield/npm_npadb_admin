class EntityName(object):
    """
    Entities have names but
    we may want to do more with those names than simply store them or display them as is.
    """
    def __init__(self, name: str):
        self.name = name

    def display_name(self):
        return self.name

    def index_name(self):
        return self.name


class PlaceName(EntityName):
    pass
