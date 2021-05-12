"""Console script for polly_python."""
import sys
from polly_python.authenticate import PollyLogin, PollyLogout

def main():
    if len(sys.argv) == 2 and sys.argv[1]=='login':
        PollyLogin()
    elif len(sys.argv) == 2 and sys.argv[1]=='logout':
        PollyLogout()
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
