class TransactionItem:
    '''
    Class TransactionItem, transaction item class
    '''
    def __init__(self, label, version = 0, read_ts = 0, write_ts = 0):
        '''
            Initiate needed variables
            params:
            label = item label
            version = item version
            read_ts = item read timestamp
            write_ts = item write timestamp
            dependents = all the id which already read or wrote (to handle cascading rollback)
        '''
        self.label: int = label
        self.version: int = version
        self.read_ts: int = read_ts
        self.write_ts: int = write_ts
        self.dependents: list[int] = []

    def __str__(self):
        '''
            Overriding to print as a string
        '''
        return f'{self.label}{self.version}({self.read_ts},{self.write_ts})'


class Operation:
    '''
    Class Operation, all possible ops on a transaction
    '''
    def __init__(self, str: str):
        '''
            Initiate needed variables
            params:
            str = readed string from file, example R1(A)
        '''
        temp = str.split("(")
        self.operation = str[0]
        self.t_num = int(temp[0][1:])
        if len(temp) > 1:
            self.item = TransactionItem(temp[1][:-1])
            
    def __str__(self):
        '''
            Overriding to print as a string
        '''
        if (self.operation == "W"):
            return f'T{self.t_num} on {self.item.label} to DB  '
        else:
            return f'T{self.t_num} on {self.item.label} from DB'


class MVCCTransaction():
    '''
    Class MVCCTransaction, state as a transaction object
    '''
    def __init__(self, t_num, timestamp):
        '''
            Initiate needed variables
            params:
            t_num = transaction number
            timestamp = transaction timestamp (TS)
            arr_process = set of operation exist on that transaction
            created_items = all items (different versions) created in this transaction
        '''
        self.t_num: int = t_num
        self.timestamp: int = timestamp
        self.arr_process: list[Operation] = []
        self.created_items: list[TransactionItem] = []
        
    def __str__(self):
        return f"{self.t_num}: [{';'.join(map(lambda x: str(x), self.arr_process))}]"
        

class VersionControl:
    '''
    Class VersionControl, handle multiple data versions
    '''
    def __init__(self):
        '''
            Initiate needed variables
        '''
        self.map: dict[str, list[TransactionItem]] = {}

    def add_new_version(self, item: TransactionItem):
        '''
            Add new version to the version controller
            params:
            item = transaction item with different version
        '''
        # If label not exist before, add new label and its item
        if item.label not in self.map:
            self.map.update({item.label: [item]})
        # Else, just append item on the existed label
        else:
            self.map[item.label].append(item)

    def get(self, label: str):
        '''
            Get all the version on spesific label name
            params:
            label = item name
        '''
        return self.map[label]
    
    def print_label(self, label: str):
        '''
            Print all the version on specific label name
            params:
            label = item name
        '''
        transaction = self.get(label)
        result_str = ""
        result_str += f"Version {label} : "
        for item in transaction:
            result_str += f"{item}; "
        print(result_str)
