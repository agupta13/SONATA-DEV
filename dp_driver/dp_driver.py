#!/usr/bin/env python


from p4_target import P4Target


class DataplaneDriver(object):
    def __init__(self):
        pass

    def is_supportable(self, application, target):
        return False

    def get_cost(self, target):
        return 999

    def execute(self, application, target):
        pass


def main():
    dp_driver = DataplaneDriver()


if __name__ == '__main__':
    main()
