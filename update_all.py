from refreshBovada import update_bovada
from newVSIN import update_vsin
from refreshESPN import update_ESPN
from combineDBs import combine_all


def update_and_combine():
    update_bovada()
    update_vsin()
    update_ESPN()

    combine_all()


if __name__ == '__main__':
    update_and_combine()