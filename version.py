import subprocess


def get_version():
    try:
        return subprocess.check_output("git rev-parse --short HEAD".split(" "))
    except subprocess.CalledProcessError:
        print("Unable to get version number from git tags")
        exit(1)


if __name__ == "__main__":
    print(get_version())
