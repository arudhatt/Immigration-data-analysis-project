import sys
import refugee


def main():
    # with open("loaddata.py", "rb") as source_file:
    #     code = compile(source_file.read(), "loaddata.py", "exec")
    #     exec(code)
    ## exec(compile(open("loaddata.py").read(), "loaddata.py", "exec"))
    print("Choose data you want to query: \n1 - Refugee Data \n2 - Immigration Data")
    ch = sys.stdin.read(1)
    if ch == "1":
        refugee.ref()


if __name__ == "__main__":
    main()