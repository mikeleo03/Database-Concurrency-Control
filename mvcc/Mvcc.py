from FileHandler import FileHandler
from Lib import MVCCTransaction, Operation, VersionControl, TransactionItem

class MVCC:
    '''
    Class MVCC, implementing MVCC Timestamp Ordering Protocol functionalities
    '''
    def __init__(self, file_handler: FileHandler):
        '''
            Initiate needed variables
            params:
            file_handler = processing the input from txt
        '''
        self.file_handler = file_handler
        self.current_timestamp = 0
        self.transactions: list[MVCCTransaction] = []
        self.operations: list[Operation] = []
        self.version_controller = VersionControl()

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

    def prepare_item(self, op: Operation):
        '''
            Setup the initial conndition of MVCC Timestamp Ordering
            params:
            op = operation
        '''
        # Check if the operation is not commit and not exist in version control
        if (op.operation != "C" and op.item.label not in self.version_controller.map):
            new_item = TransactionItem(op.item.label)
            self.version_controller.add_new_version(new_item)

    def prepare(self, op: Operation):
        '''
            Preparation of everytime the operation need to be executed
            params:
            op = operation
        '''
        # Check whether the transaction already exist before
        trx = self.find_transaction(op.t_num)
        
        # If not exist, create a new transaction with timestamp = t_num
        if trx is None:
            new_trx = MVCCTransaction(op.t_num, op.t_num)
            new_trx.arr_process.append(op)      # Add to its array process
            self.transactions.append(new_trx)   # Add to global MVCC transaction array
            self.current_timestamp += 1         # Add new timestamp
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
        # Read operation
        if (op.operation == "R"):
            success = self.process_read(op)
            print('\033[92m' + "[READ]       | " if success else '\033[91m' + "[READ]       | ", end="")
            print(op, end="")
            print(" | ", end="")
            self.version_controller.print_label(op.item.label)
        # Write operation
        elif (op.operation == "W"):
            success = self.process_write(op)
            print('\033[92m' + "[WRITE]      | " if success else '\033[91m' + "[WRITE]      | ", end="")
            print(op, end="")
            print(" | ", end="")
            self.version_controller.print_label(op.item.label)
        # Commit operation
        elif (op.operation == "C"):
            success = self.process_commit(op)
            print('\033[92m' + f"[COMMIT]     | T{op.t_num}" if success else '\033[91m' + f"[COMMIT]     | T{op.t_num}")

        return success

    def process_read(self, op: Operation):
        '''
            Simulate read transaction from DB
            params:
            op = operations to be read
        '''
        # Get the correct version from version controller
        versions = self.version_controller.get(op.item.label)
        # Get the operation timestamp
        op_ts = self.find_transaction(op.t_num).timestamp
        
        for version in reversed(versions):
            # Holds R-TS(Qk) < TS(Ti), set R-TS(Qk) = TS(Ti)
            if version.read_ts < op_ts:
                version.read_ts = op_ts
                # Add to the dependents array, in case rollback needed
                version.dependents.append(op.t_num)
                break
            
        return True

    def process_write(self, op: Operation):
        '''
            Simulate write transaction to DB
            params:
            op = operations to be wrote
        '''
        # Get the correct version from version controller
        versions = self.version_controller.get(op.item.label)
        # Get the operation timestamp
        op_ts = self.find_transaction(op.t_num).timestamp
        
        for version in reversed(versions):
            # Holds TS(Ti) < R-TS(Qk), then transaction Ti is rolled back
            if (op_ts < version.read_ts):
                continue
            
            # Holds TS(Ti) = W-TS(Qk), the contents of Qk are overwritten
            if (op_ts == version.write_ts):
                # Return True
                return True

            # Otherwise, a new version Qi of Q is created
            # W-TS(Qi) and R-TS(Qi) are initialized to TS(Ti)
            new_version = TransactionItem(op.item.label, version = op_ts, read_ts = op_ts, write_ts = op_ts)
            
            # Add to the dependents array, in case rollback needed
            new_version.dependents.append(op.t_num)
            
            # Add to new version on controller and new created items on that transaction
            self.version_controller.add_new_version(new_version)
            self.find_transaction(op.t_num).created_items.append(new_version)

            # Return True
            return True

        # Else would be rolled back
        return False

    def process_commit(self, _: Operation):
        '''
            Simulate transaction commit
            Since protocol guarantees serializability, always return true
        '''
        return True

    def handle_abort(self, t_num: int) -> list[int]:
        '''
            Handle abort and simulate full rollback (including cascading) mechanism
            params:
            t_num = transaction id
        '''
        print('\033[93m' + f"[ABORT]      | T{t_num} rolled back")
        
        # Get the transaction and add it to aborted transaction array
        trx = self.find_transaction(t_num)
        aborted: list[int] = [t_num]

        # Doing casacding rollback based on created_items data on that transaction
        for item in trx.created_items:
            # Get all the version with the item label and remove it
            versions = self.version_controller.get(item.label)
            versions.remove(item)

            # Using recursive approach to trace tree of dependents item to cascading rollback
            for id in item.dependents:
                if id == t_num:
                    continue
                
                # Add other transaction dependents to the aborted transaction array
                cur_aborted = self.handle_abort(id)
                new_aborted = []
                for id in cur_aborted:
                    if id not in aborted:
                        new_aborted.append(id)
                aborted.extend(new_aborted)

        # Return array of transaction id aborted
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
            trx.timestamp = self.current_timestamp
            trx.created_items = []

            # Re-run all the operations on that transaction
            for op in trx.arr_process:
                self.run_operation(op)
                
    def run(self):
        '''
            Run the MVCC
        '''
        # Read the file configuration and add it to all operations
        while True:
            str_line = self.file_handler.next_line()
            if str_line == '':
                break
            self.operations.append(Operation(str_line))

        # Prepare the initial condition of the Timestamp Ordering
        for op in self.operations:
            self.prepare_item(op)

        # Do every single operation sequencially
        for op in self.operations:
            self.prepare(op)
            self.run_operation(op)
        
        # Restore the terminal color
        print('\033[0m')
