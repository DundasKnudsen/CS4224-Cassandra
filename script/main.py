import sys


def run_xacts(xact_file_dir, xact_id):
    sys.stderr.write(xact_file_dir + " - " + xact_id + "\n")


def main():
    xact_file_dir = sys.argv[1]
    xact_id = sys.argv[2]
    run_xacts(xact_file_dir, xact_id)


if __name__ == "__main__":
    main()
