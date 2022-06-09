import re


def to_snake_case(camelCase):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camelCase).lower()