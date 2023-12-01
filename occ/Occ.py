import math
from FileHandler import FileHandler
from Lib import OCCTransaction, Operation

class OCC:
    '''
    Class OCC, implementing OCC Protocol functionalities
    '''
    def __init__(self, file_handler: FileHandler) -> None:
        '''
            Initiate needed variables
            params:
            file_handler = processing the input from txt
        '''
        self.file_handler = file_handler
        self.current_timestamp = 0
        self.transactions: list[OCCTransaction] = []
        self.operations: list[Operation] = []
        self.local_vars: list[str] = []
        
    def find_transaction(self, t_num: int):
        '''
            Find transaction with spesific id
            params:
            t_num = transaction id looked for
        '''
        for trx in self.transactions:
            if trx.t_num == t_num:
                return trx
        return None
        
    def prepare(self, op: Operation):
        '''
            Setup the initial condition of OCC
            params:
            op = operation
        '''
        # Check whether the transaction already exist before
        trx = self.find_transaction(op.t_num)
        
        # If not exist, create a new transaction
        if trx is None:
            new_trx = OCCTransaction(op.t_num)
            new_trx.arr_process.append(op)     
            new_trx.timestamps["start"] = self.current_timestamp
            self.transactions.append(new_trx)
            self.current_timestamp += 1
        # Else, just add the operation to its array process
        else:
            trx.arr_process.append(op)
            
    def process(self, op: Operation):
        '''
            Executing the operations
            params:
            op = operation
        '''
        success = False
        trx = self.find_transaction(op.t_num)
        read_set = trx.read_set
        write_set = trx.write_set
        
        # Read operation
        if (op.operation == "R"):
            success = self.process_read(op)
            print('\033[92m' + "[READ]       | " if success else '\033[91m' + "[READ]       | ", end="")
            print(op, end="")
            print(f" | Read Set T{op.t_num} : {read_set}")
        # Write operation
        elif (op.operation == "W"):
            success = self.process_tempwrite(op)
            print('\033[92m' + "[TEMPWRITE]  | " if success else '\033[91m' + "[TEMPWRITE]  | ", end="")
            print(op, end="")
            print(f" | Write Set T{op.t_num} : {write_set}")
        # Commit operation
        elif (op.operation == "C"):
            success = self.process_validate(op)
            if success:
                print('\033[92m' + f"[VALIDATE]   | T{op.t_num}")
                self.write(op)
            else:
                print('\033[91m' + f"[VALIDATE]   | T{op.t_num}")

        return success
    
    def process_read(self, op: Operation):
        '''
            Simulate read transaction from DB
            params:
            op = operations to be read
        '''
        # Add the timestamp
        self.current_timestamp += 1 
        
        # Get the operation transaction
        trx = self.find_transaction(op.t_num)
        
        # Add to read_set tx
        if op.item not in trx.read_set:
            trx.read_set.append(op.item)
            
        return True
    
    def process_tempwrite(self, op: Operation):
        '''
            Simulate temp write transaction before actually write to DB
            params:
            op = operations to be wrote temporary
        '''
        # Add the timestamp
        self.current_timestamp += 1
        # Write to local variable
        self.local_vars.append(op.item)
        
        # Get the operation transaction
        trx = self.find_transaction(op.t_num)
        
        # Add to write_set tx
        if op.item not in trx.write_set:
            trx.write_set.append(op.item)
            
        return True
    
    def process_commit(self, op: Operation):
        '''
            Simulate transaction commit
            op = operations to be commited
        '''
         # Add the timestamp
        self.current_timestamp += 1
        
        # Get the operation transaction
        trx = self.find_transaction(op.t_num)
        # Set the finish timestamp
        trx.timestamps["finish"] = self.current_timestamp
        
        print('\033[92m' + f"[COMMIT]     | T{op.t_num}")
    
    def process_validate(self, op: Operation):
        '''
            Validation mechanism over transaction before finally commited
            params:
            op = operations to be validated
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Get the operation transaction
        trx = self.find_transaction(op.t_num)
        # Set the transaction validation timestamp
        trx.timestamps["validation"] = self.current_timestamp
        
        # Get all the transaction ids
        transaction_nums = [transaction.t_num for transaction in self.transactions]
        
        # Validation check based on timestamp value criterion
        for current_t_num in transaction_nums:
            if current_t_num != op.t_num:
                # Get the operation transaction
                ti = self.find_transaction(current_t_num)
                tj = self.find_transaction(op.t_num)
                # Get the timestamp values
                ti_validationTS = ti.timestamps["validation"]
                ti_finishTS = ti.timestamps["finish"]
                tj_startTS = tj.timestamps["start"]
                tj_validationTS = tj.timestamps["validation"]
                
                # If for all Ti with TS(Ti) < TS(Tj) and TS(Ti) = ValidationTS(Ti)
                if ti_validationTS < tj_validationTS:
                    # Holds finishTS(Ti) < startTS(Tj), then Tj valid, can be commited
                    if ti_finishTS < tj_startTS:
                        return True
                    
                    # Holds startTS(Tj) < finishTS(Ti) < validationTS(Tj)
                    elif ti_finishTS != math.inf and (tj_startTS < ti_finishTS and ti_finishTS < tj_validationTS):
                        # And set of data items written by Ti does not intersect with the set of data items read by Tj
                        write_set_ti = ti.write_set
                        read_set_tj = tj.read_set
                        is_element_intersect = False
                        for v in write_set_ti:
                            if v in read_set_tj:
                                is_element_intersect = True
                                break
                        
                        # If intersect, then invalid, Tj is aborted
                        if is_element_intersect:
                            return False
                        # Else, then Tj valid, can be commited
                        else:
                            return True
                        
                    # Otherwise, validation fails, Tj is aborted
                    else:
                        return False
                    
        return True
    
    def write(self, op: Operation):
        '''
            Simulate write transaction to DB
            params:
            op = operation to be written
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Get all the write_set and write it to DB
        trx = self.find_transaction(op.t_num)
        for var in trx.write_set:
            print('\033[92m' + f"[WRITE]      | T{op.t_num} on {var} to DB")

        # End all the write process with transaction commit
        self.process_commit(op)
    
    def handle_abort(self, t_num: int) -> list[int]:
        '''
            Handle abort and simulate full rollback (including cascading) mechanism
            params:
            t_num = transaction id
        '''
        print('\033[93m' + f"[ABORT]      | T{t_num} rolled back")
        
        # Get the transaction and add it to aborted transaction array
        aborted: list[int] = [t_num]
        
        return aborted
            
    def run_operation(self, op: Operation):
        '''
            Run every single operations exist on every transaction exists
            params:
            op = operation
        '''
        # Process the operation and get the process status
        success = self.process(op)
        if success:
            return

        # If not sucess, handle the abort mechanism based on transaction id
        aborted_ids = self.handle_abort(op.t_num)
        
        # For all aborted transaction id
        for id in aborted_ids:
            # Add the timestamp
            self.current_timestamp += 1

            # Set the new transaction timestamp
            trx = self.find_transaction(id)
            trx.read_set = []
            trx.write_set = []
            trx.timestamps = {
                "start": self.current_timestamp,
                "validation": math.inf,
                "finish": math.inf
            }

            # Re-run all the operations on that transaction
            for op in trx.arr_process:
                self.run_operation(op)
        
    def run(self):
        '''
            Run the OCC
        '''
        # Read the file configuration and add it to all operations
        while True:
            str_line = self.file_handler.next_line()
            if str_line == '':
                break
            self.operations.append(Operation(str_line))

        # Do every single operation sequencially
        for op in self.operations:
            self.prepare(op)
            self.run_operation(op)
        
        # Restore the terminal color
        print('\033[0m')
