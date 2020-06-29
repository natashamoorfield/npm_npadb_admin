class EntityName(object):
    """
    Entities have names but
    we may want to do more with those names than simply store them or display them as is.
    """
    # TODO A comprehensive punctuation list
    # Some punctuation characters (such as comma, underscore and slash, should be treated as word separators
    # Others should be ignored altogether
    PUNCTUATION = {
        34: None,  # Double Quote
        39: None,  # Apostrophe/Single Quote
        44: 32,    # Comma
        45: None,  # Hyphen/Dash/Minus
        46: None,  # Full Stop
        95: 32     # Underscore
    }

    ARTICLES = ['a', 'an', 'the', 'ye']

    # TODO Fully codify the 'special index' values
    SI_DROP_LEADING_ARTICLE = 1

    def __init__(self, name: str):
        self.name_as_given = name
        self.elements = self.stripped_elements()

    def display_name(self):
        return self.name_as_given

    def index_name(self, special_index: int):
        return_value = self.stripped_elements()
        if special_index & EntityName.SI_DROP_LEADING_ARTICLE:
            return_value = EntityName.without_leading_article(return_value)
        return ''.join(return_value)

    def stripped_elements(self):
        s = self.name_as_given.translate(self.PUNCTUATION)
        return [element.lower() for element in s.split()]

    @staticmethod
    def without_leading_article(elements):
        if elements[0] in EntityName.ARTICLES:
            return elements[1:]
        return elements


class PlaceName(EntityName):
    pass


if __name__ == "__main__":
    TEST_DATA = [
        'Natasha Moorfield',
        'Cindi.Vapid',
        'The  Hare and Hounds',
        'Michael O\'Weary'
    ]
    for item in TEST_DATA:
        my_name = EntityName(item)
        print(my_name.index_name(EntityName.SI_DROP_LEADING_ARTICLE))
