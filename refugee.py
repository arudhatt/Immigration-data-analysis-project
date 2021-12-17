import sys


def ref():
    print(
        """
    Choose one of the following queries:\n
    1 - Asylum Seekers Data\n
    2 - Asylum Seekers Monthly Data\n
    3 - Demographics Data\n
    4 - Persons of concern Data\n
    5 - Resettlement Data\n
    6 - Time Series Data
    """
    )
    ch = sys.stdin.read(1)
