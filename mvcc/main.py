import sys
from Mvcc import MVCC
from FileHandler import FileHandler

def main():
    if len(sys.argv) != 2:
        raise Exception('Usage : python3 main.py <filename>')

    file_handler = FileHandler(sys.argv[1])
    mvcc = MVCC(file_handler)
    mvcc.run()

if __name__ == '__main__':
    try:
        print("--- Simulating MVCC on DB ---")
        print(f"\nResult from MVCC Timestamp Ordering Protocol: ")
        main()
    except Exception as e:
        print(e)

