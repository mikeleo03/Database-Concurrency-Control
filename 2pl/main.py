import sys
from FileHandler import FileHandler
from twoPL import TwoPL

# Main program
def main():
    if len(sys.argv) != 2:
        raise Exception('Usage : python3 main.py <filename>')
   
    transactions = FileHandler(sys.argv[1]).lines
    print(f"\nSet of transaction read from {sys.argv[1]}:")
    print(transactions)
    for i in range(len(transactions)) :
        transactions[i] = transactions[i].strip()
    twoPhase = TwoPL()
    twoPhase.simulate(transactions)

if __name__ == "__main__":
    print("--- Simulating Two Phase Locking on DB ---")
    print(f"\nResult from Two Phase Locking Protocol: ")
    main()