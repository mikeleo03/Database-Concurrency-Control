from FileHandler import FileHandler
from twoPL import TwoPL

# Main program
if __name__ == "__main__":
    print("--- Simulating 2 Phase Locking on DB ---")
    file_input = input("Enter the .txt file name: ")

    transactions = FileHandler(file_input).lines
    print(f"\nSet of transaction read from {file_input}:")
    print(transactions)
    for i in range(len(transactions)) :
        transactions[i] = transactions[i].strip()
    twoPhase = TwoPL()
    twoPhase.simulate(transactions)
