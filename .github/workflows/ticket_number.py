import os, sys


def main(argv):
    s = argv
    print(s.split(":")[0].strip())


if __name__ == "__main__":
    main(sys.argv[1])
