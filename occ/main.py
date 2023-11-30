import sys
from Occ import OCC
from FileHandler import FileHandler

def main():
    if len(sys.argv) != 2:
        raise Exception('Usage : python3 main.py <filename>')

    file_handler = FileHandler(sys.argv[1])
    occ = OCC(file_handler)
    occ.run()

# Main program
if __name__ == "__main__":
    try:
        print("--- Simulating OCC on DB ---")
        print(f"\nResult from OCC Protocol: ")
        main()
    except Exception as e:
        print(e)
        
        