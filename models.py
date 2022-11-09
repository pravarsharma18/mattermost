import random
import string
from pprint import pprint


size_of_id = 26


def generate_id(size_of_id):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size_of_id))


class Options:
    colors = ['propColorOrange', 'propColorPurple', 'propColorBlue', 'propColorGreen',
              'propColorPurple', 'propColorRed', 'propColorGray', 'propColorBrown', 'propColorPink']

    def __init__(self, **kwargs):
        self.value = kwargs['value']

    def get_options(self):
        data = {
            "id": generate_id(size_of_id),
            "color": random.choices(self.colors)[0],
            "value": self.value
        }
        return data


class CardProperties:
    def __init__(self, **kwargs):
        self.name = kwargs['name']
        self.type = kwargs['type']
        self.options = kwargs.get('option_names')

    def get_cards(self):
        options = []
        if self.options:
            for i in self.options:
                opt = Options(value=f'{i}')
                options.append(opt.get_options())
        data = {
            "id": generate_id(size_of_id),
            "name": self.name,
            "type": self.type,
            "options": options
        }
        return data


if __name__ == '__main__':
    li = []
    card = CardProperties(name="Select", type="select", option_names=[
        "Backlog", "Open", "In Progress", "PR-Submitted", "In Review", "Re-open", "Closed"])
    # c = CardProperties(name="Type", type="select", option_names=[
    #     "Press Release", "Sponsored Post", "Customer Story", "Product Release", "Partnership", "Feature Announcement", "Article"])

    # d = CardProperties(name="Assignee", type="person")

    # li.append(b.get_cards())
    # li.append(c.get_cards())
    # li.append(d.get_cards())
    # CardProperties(name="Select", type="select", option_names=[])
    li.append(card.get_cards())
    pprint(card.get_cards())
