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
                        print('\033[93m' + f"[READ]   | T{num} on {item} from DB | Lock held by younger transaction")
                        print('\033[93m' + f"[ABORT]  | T{lockNum} rolled back")
                        self._rollback(lockNum)
                    else :
                        return False
            self.lock_list[item].append(('S', num))
            return True
        elif op == 'W' : # Operasi write
            for (type, lockNum) in held_keys :
                if (num != lockNum) :
                    if (self.transactionOrder.index(num) < self.transactionOrder.index(lockNum)) :
                        print('\033[93m' + f"[WRITE]  | T{num} on {item} to DB | Lock held by younger transaction")
                        print('\033[93m' + f"[ABORT]  | T{lockNum} rolled back")
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
        op = str.upper(trans[0])
        num = trans[1]
        if (op == 'R' or op =='W') :
            item = trans[3]
            lockSuccess = self._acquire_lock(op, num, item)
            if not(lockSuccess) :
                if (op == 'R') :
                    print('\033[91m' + f"[READ]   | T{num} on {item} from DB | Inserted into queue")
                else :
                    print('\033[91m' + f"[WRITE]  | T{num} on {item} to DB | Inserted into queue")
                return (1,num)
            else :
                if (op == 'R') :
                    print('\033[92m' + f"[READ]   | T{num} on {item} from DB")
                else :
                    print('\033[92m' + f"[WRITE]  | T{num} on {item} to DB")
        elif str.upper(op) == 'C' : # Operasi commit
            # Buka semua lock yang dipegang transaksi yang dicommit
            self._release_locks(num)
            self.final_result.append(op + num)
            print('\033[92m' + f"[COMMIT] | T{num}")
            return (2, num)
        else :
            print('\033[91m' + "[ERROR]  | Command not valid, raising exception")
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
                        print('\033[0m' + "[INFO]   | Current queue :", self.queue)
                        print('\033[0m' + "[INFO]   | Current lock table :")
                        for item in self.lock_list :
                            print("[LOCK]   |", item, ": ", end="")
                            for (lockType, lockNum) in self.lock_list[item] :
                                print(lockType+f"L({lockNum})", end=" ")
                            print()
                        print()
                        if (last_result[0] == 1) :
                            locked_num_list.append(last_result[1])
                            self.queue.append(transQ)
                    else :
                        self.queue.append(transQ)

            op = comm[0]
            num = comm[1]
            if (not(num in self.transactionOrder)) :
                self.transactionOrder.append(num)
            item = None
            if (str.upper(op) == 'R' or str.upper(op) == 'W') :
                item = comm[3]
                if (not(item in self.lock_list.keys())) :
                    self.lock_list[item] = []

            # Periksa apakah transaksi dengan nomor tersebut sedang dilock, jika iya, masukkan ke queue
            isLocked = False
            for transQ in self.queue :
                if (num == transQ[1]) :
                    print('\033[91m' + f"[LOCKED] | T{num} is waiting for lock")
                    self.queue.append(comm)
                    isLocked = True
                    break
            
            if (not(isLocked)) :
                last_result = self._execute(comm)
                if (last_result[0] == 1) :
                    self.queue.append(comm)
            print('\033[0m' + "[INFO]   | Current queue :", self.queue)
            print('\033[0m' + "[INFO]   | Current lock table :")
            for item in self.lock_list :
                print("[LOCK]   |", item, ": ", end="")
                for (lockType, lockNum) in self.lock_list[item] :
                    print(lockType+f"L({lockNum})", end=" ")
                print()
            print()

        if (len(self.queue) > 0) :
            print('\033[94m' + "[INFO]   | Queue not empty, re-running simulation on queue")
            leftOverTransact = []
            for trans in self.queue :
                leftOverTransact.append(trans)
            self.queue = []
            self.simulate(leftOverTransact)
        else :
            print('\033[96m' + "[FINAL]  | Final result :", self.final_result)
            # Reset variables
            self.lock_list = {}
            self.queue = []
            self.final_result = []
            self.transactionOrder = []
            print('\033[0m')