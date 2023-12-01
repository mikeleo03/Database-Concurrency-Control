from FileHandler import FileHandler

class TwoPL :
    def __init__(self) -> None:
        self.lock_list = {}
        self.queue = []
        self.final_result = []
        self.transactionOrder = []

    def _rollback(self,num) :
        '''
        Melakukan rollback pada transaksi bernomor num
        '''
        self._release_locks(num)
        tempQueue = []
        for trans in list(self.final_result) :
            if trans[1] == num :
                tempQueue.append(trans)
                self.final_result.remove(trans)
        for i in range(len(tempQueue)-1, -1, -1) :
            self.queue.insert(0,tempQueue[i])

    def _acquire_lock(self, op, num, item) :
        held_keys = self.lock_list.get(item)
        if op == 'R' : # Operasi read
            for (type, lockNum) in held_keys :
                if (type =='X' and num != lockNum) :
                    if (self.transactionOrder.index(num) < self.transactionOrder.index(lockNum)) :
                        self._rollback(lockNum)
                    else :
                        return False
            self.lock_list[item].append(('S', num))
            return True
        elif op == 'W' : # Operasi write
            for (type, lockNum) in held_keys :
                if (num != lockNum) :
                    if (self.transactionOrder.index(num) < self.transactionOrder.index(lockNum)) :
                        self._rollback(lockNum)
                    else :
                        return False
            if ('S', num) in list(held_keys) : # Jika sudah punya shared lock, lakukan upgrade
                self.lock_list[item].remove(('S', num))
            self.lock_list[item].append(('X', num))
            return True
    
    def _release_locks(self, num) :
         for item in self.lock_list :
                held_keys = self.lock_list[item]
                for (type, lockNum) in held_keys :
                    if (lockNum == num) :
                        held_keys.remove((type, lockNum))

    def _execute(self, trans) :
        '''
        Mengecek eksekusi operasi yang diberikan.
        Mengembalikan tuple dengan format (<kode hasil>, <nomor transaksi yang dieksekusi>).
        Berikut kode hasil yang digunakan dan artinya :
        - 0 = berhasil tanpa masalah
        - 1 = operasi masuk ke queue
        - 2 = terjadi unlock
        '''
        print("Execute", trans)
        op = str.upper(trans[0])
        num = trans[1]
        if (op == 'R' or op =='W') :
            item = trans[3]
            lockSuccess = self._acquire_lock(op, num, item)
            if not(lockSuccess) :
                print("Lock tidak berhasil diambil")
                return (1,num)
        elif str.upper(op) == 'C' : # Operasi commit
            # Buka semua lock yang dipegang transaksi yang dicommit
            self._release_locks(num)
            self.final_result.append(op + num)
            return (2, num)
        else :
             print("Command not valid, aborting simulation")
             raise Exception()
        self.final_result.append(op + num + "(" + str(item) + ")")
        return (0,num)
    
    def simulate(self, transaction) :
        '''
        Lakukan simulasi two phase locking menggunakan transaksi yang diberikan.
        Penanganan deadlock menggunakan strategi wait-die
        Params : 
        - transaction : array of string, berisi daftar command, nomor transaksi, beserta item yang diperlukan. Contoh : [R2(A),W1(B),C1,C2]
        - allItems : array of string, berisi semua item yang ada di basis data simulasi
        '''
        # Inisialisasi daftar lock yang sedang dipegang
        last_result = (0,0)
        for comm in transaction :
            # Jika baru ada lock yang dibuka, eksekusi queue dulu
            if (last_result[0] == 2) :
                locked_num_list = []
                for transQ in list(self.queue) :
                    self.queue.remove(transQ)
                    if (not (transQ[1] in locked_num_list)) :
                        last_result = self._execute(transQ)
                        if (last_result[0] == 1) :
                            locked_num_list.append(last_result[1])
                            self.queue.append(transQ)
                    else :
                        self.queue.append(transQ)
                print("Queueu setelah eksekusi setelah unlock :", self.queue)

            op = comm[0]
            num = comm[1]
            if (not(num in self.transactionOrder)) :
                self.transactionOrder.append(num)
            item = None
            if (str.upper(op) == 'R' or str.upper(op) == 'W') :
                item = comm[3]
                if (not(item in self.lock_list.keys())) :
                    self.lock_list[item] = []

            # Periksa apakah transaksi dengan nomor tersebut sedang dilock, jika iya, masukkan ke queueu
            isLocked = False
            for transQ in self.queue :
                if (num == transQ[1]) :
                    self.queue.append(comm)
                    isLocked = True
                    break
            
            if (not(isLocked)) :
                last_result = self._execute(comm)
                if (last_result[0] == 1) :
                    self.queue.append(comm)
            print("Curr op :", op, num, item)
            print("Curr queue :", self.queue)
            print("Curr lock list:", self.lock_list)
            print("Temp result :",  self.final_result)
            print()

        if (len(self.queue) > 0) :
            print("Queue not empty")
            leftOverTransact = []
            for trans in self.queue :
                leftOverTransact.append(trans)
            self.queue = []
            self.simulate(leftOverTransact)
        
        print("Final :", self.final_result)
        print("Lock list :", self.lock_list)
        print("Queue :", self.queue)
        # Reset variables
        self.lock_list = {}
        self.queue = []
        self.final_result = []
        self.transactionOrder = []

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
