import math

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
            self.item = temp[1][:-1]
            
    def __str__(self):
        '''
            Overriding to print as a string
        '''
        if (self.operation == "W"):
            return f'T{self.t_num} on {self.item} to LOCAL'
        else:
            return f'T{self.t_num} on {self.item} from DB '
        
        
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
        self.t_num: int = t_num                  
        self.read_set: list[str] = []   
        self.write_set: list[str] = []             
        self.timestamps = {                 
            "start": math.inf,
            "validation": math.inf,
            "finish": math.inf
        }
        self.arr_process: list[Operation] = []
        