import math

class OCCTransaction:
    '''
    Class OCCTransaction, to handle OCC transaction components
    '''
    def __init__(self, t_num: int) -> None:
        '''
            Initiate needed variables
            params:
            t_num = transaction number
        '''
        self.t_num = t_num                  # Transaction number
        self.read_set = []                  # Read set
        self.write_set = []                 # Write set
        self.timestamps = {                 # Timestamps, init with infinite
            "start": math.inf,
            "validation": math.inf,
            "finish": math.inf
        }

class OCC:
    '''
    Class OCC, implementing OCC Algorithm functionalities
    '''
    def __init__(self, transactions_string: str) -> None:
        '''
            Initiate needed variables
            params:
            transactions_string = transactions as a set of string
        '''
        self.input_transactions = self.parse_transactions(transactions_string)
        self.local_vars = []
        self.current_timestamp = 0
        self.transactions = {}
        self.rolledback_transaction_nums = []

    def parse_transactions(self, transactions_string: str):
        '''
            Parse string transactions from user input
            params:
            transactions_string = transactions as a set of string
        '''
        parsed_transactions = []
        sequences = transactions_string.replace(' ', '').split(';')
        for command in sequences:
            parsed_cmd = {}
            # Parse the command spesific on read and write syantax
            # Rx(A) means read in Transaction x on A
            # Wx(A) means write in Transaction x on A
            if (command[0].upper() == 'R' or command[0].upper() == 'W') and '(' in command and ')' in command:
                par_open_idx = command.index('(')
                par_close_idx = command.index(')')
                if par_open_idx > par_close_idx:
                    print("INVALID SEQUENCE (parenthesis)")
                    break
                
                # Get the action values
                if command[0].upper() == 'R':
                    parsed_cmd["action"] = "read"
                elif command[0].upper() == 'W':
                    parsed_cmd["action"] = "write"
                
                # Get the transaction number and items
                t_number = command[1:par_open_idx]
                parsed_cmd["t_num"] = int(t_number)
                parsed_cmd["item"] = command[par_open_idx+1:par_close_idx]
            
            # Parse the command spesific on commit syantax
            # Cx means commit in Transaction x
            elif command[0].upper() == 'C':
                parsed_cmd["action"] = "commit"
                parsed_cmd["t_num"] = int(command[1:])
                
            # Else, invalid command
            else:
                print(f"INVALID COMMAND {command}")
                break
            
            # Store on parsed transaction array
            parsed_transactions.append(parsed_cmd)
        return parsed_transactions

    def read(self, command):
        '''
            Simulate read transaction from DB
            params:
            command = transaction command, consists transaction num and item
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Read from the database
        t_num = command["t_num"]
        t_item = command["item"]
        
        # Add to read_set tx
        if t_item not in self.transactions[t_num].read_set:
            self.transactions[t_num].read_set.append(t_item)
            
        # Show all the read_set
        read_set = self.transactions[t_num].read_set
        print(f"[READ]       | T{t_num} on {t_item} from DB | READ_SET T{t_num} : {read_set}")

    def temporary_write(self, command):
        '''
            Simulate temp write transaction before actually write to DB
            params:
            command = transaction command, consists transaction num and item
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Write to local variable
        t_num = command["t_num"]
        t_item = command["item"]
        self.local_vars.append(t_item)
        
        # # Add to write_set tx
        if t_item not in self.transactions[t_num].write_set:
            self.transactions[t_num].write_set.append(t_item)
            
        # Show all the write_set
        write_set = self.transactions[t_num].write_set
        print(f"[TEMPWRITE]  | T{t_num} on {t_item} to LOCAL | WRITE_SET T{t_num} : {write_set}")
        
    def validate(self, command):
        '''
            Validation mechanism over transaction before finally commited
            params:
            command = transaction command, consists transaction num and item
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Set the transaction validation timestamp
        t_num = command["t_num"]
        self.transactions[t_num].timestamps["validation"] = self.current_timestamp
        
        # Validation check based on timestamp value criterion
        is_valid = True
        for current_t_num in self.transactions.keys():
            if current_t_num != t_num:
                # Get the timestamp values
                ti_validation_timestamp = self.transactions[current_t_num].timestamps["validation"]
                ti_finish_timestamp = self.transactions[current_t_num].timestamps["finish"]
                tj_start_timestamp = self.transactions[t_num].timestamps["start"]
                tj_validation_timestamp = self.transactions[t_num].timestamps["validation"]
                
                # If for all Ti with TS(Ti) < TS(Tj) and TS(Ti) = ValidationTS(Ti)
                if ti_validation_timestamp < tj_validation_timestamp:
                    # Holds finishTS(Ti) < startTS(Tj), then Tj valid, can be commited
                    if ti_finish_timestamp < tj_start_timestamp:
                        pass
                    
                    # Holds startTS(Tj) < finishTS(Ti) < validationTS(Tj)
                    elif ti_finish_timestamp != math.inf and (tj_start_timestamp < ti_finish_timestamp and ti_finish_timestamp < tj_validation_timestamp):
                        # And set of data items written by Ti does not intersect with the set of data items read by Tj
                        write_set_ti = self.transactions[current_t_num].write_set
                        read_set_tj = self.transactions[t_num].read_set
                        is_element_intersect = False
                        for v in write_set_ti:
                            if v in read_set_tj:
                                is_element_intersect = True
                                break
                        
                        # If intersect, then invalid, Tj is aborted
                        if is_element_intersect:
                            is_valid = False
                            break
                        # Else, then Tj valid, can be commited
                        
                    # Otherwise, validation fails, Tj is aborted
                    else:
                        is_valid = False
                        break
                    
        # If valid, then write to DB
        if is_valid:
            self.write(t_num)
        # Else, transaction aborted and rollback
        else:
            # fill rollback list
            self.rolledback_transaction_nums.append(t_num)
            print(f"[ABORT]      | T{t_num} rolled back")
                
    def write(self, t_num):
        '''
            Simulate write transaction to DB
            params:
            t_num = transactions number to be written
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Get all the write_set and write it to DB
        tx = self.transactions[t_num]
        for var in tx.write_set:
            print(f"[WRITE]      | T{t_num} on {var} to DB")

        # End all the write process with transaction commit
        self.commit(t_num)
    
    def commit(self, t_num):
        '''
            Simulate transaction commit
            params:
            command = transactions number to be commited
        '''
        self.current_timestamp += 1  # Add the timestamp
        
        # Set the finish timestamp
        self.transactions[t_num].timestamps["finish"] = self.current_timestamp
        print(f"[COMMIT]     | T{t_num}")
    
    def rollback_all(self):
        '''
            Simulate full rollback mechanism
        '''
        # Rollback all the transactions
        while (len(self.rolledback_transaction_nums) > 0):
            self.current_timestamp += 1  # Add the timestamp on every rollback
            
            # Get the top transaction of the remainings to be rolled back
            rolledback_t_num = self.rolledback_transaction_nums.pop(0)
            rolledback_tx = self.transactions[rolledback_t_num]
            
            # Reset the transaction parameters
            rolledback_tx.read_set = []
            rolledback_tx.write_set = []
            rolledback_tx.timestamps = {
                "start": self.current_timestamp,
                "validation": math.inf,
                "finish": math.inf
            }
            
            # Get all the transaction commands of that transaction
            rolledback_cmd_sequence = []
            for command in self.input_transactions:
                if command["t_num"] == rolledback_t_num:
                    rolledback_cmd_sequence.append(command)
                    
            # Perform all the action based on each command value
            for rolledback_cmd in rolledback_cmd_sequence:
                # Do read
                if rolledback_cmd["action"] == "read":
                    self.read(rolledback_cmd)
                # Do temporary write
                elif rolledback_cmd["action"] == "write":
                    self.temporary_write(rolledback_cmd)
                # Do commit
                elif rolledback_cmd["action"] == "commit":
                    self.validate(rolledback_cmd)
                # Always add the timestamp everytime action done
                self.current_timestamp += 1

            self.current_timestamp += 1  # Add the final timestamp
                
    def run(self):
        '''
            Run the OCC
        '''
        # For all command in the transactions
        for command in self.input_transactions:
            # Setup the transaction number and startTS
            if command["t_num"] not in self.transactions.keys():
                self.transactions[command["t_num"]] = OCCTransaction(command["t_num"])
                self.transactions[command["t_num"]].timestamps["start"] = self.current_timestamp
            
            # Perform the action based on the command value
            # Do read
            if command["action"] == "read":
                self.read(command)
            # Do temporary write
            elif command["action"] == "write":
                self.temporary_write(command)
            # Do commit
            elif command["action"] == "commit":
                self.validate(command)
            # Always add the timestamp everytime action done
            self.current_timestamp += 1
       
        # Finally, rollback every transactions on the rolled back transaction list
        self.rollback_all()


# Main program
if __name__ == "__main__":
    print("--- Simulating OCC on DB ---")
    print("Enter input string (delimited by ;): ")
    input_string = input("")
    occ = OCC(input_string)
    print()
    occ.run()