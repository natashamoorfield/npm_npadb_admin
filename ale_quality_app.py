import random


def beer_score():
    s = random.randint(0, 10)
    for i in range(11):
        if i == 10:
            return 10
        print(f'Question {i}')
        if i == s:
            return i


print(f'    Beer Score = {beer_score()}')
