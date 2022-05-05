import graphviz
from dataclasses import dataclass
from enum import Enum
from itertools import permutations


class OperationType(Enum):
    READ = 1
    WRITE = 2
    READ_LOCK = 3
    WRITE_LOCK = 4
    COMMIT = 5
    ABORT = 6

optype_to_string = {OperationType.READ: "r",
                    OperationType.WRITE: "w",
                    OperationType.READ_LOCK: "rl",
                    OperationType.WRITE_LOCK: "wl",
                    OperationType.ABORT: "a",
                    OperationType.COMMIT: "c"}

string_to_optype = {v:k for k,v in optype_to_string.items()}

@dataclass
class Operation():
    """Class for representing single transaction operations"""
    transaction_id: int
    op_type: OperationType
    variable: str

    def __repr__(self):
        if self.op_type in [OperationType.COMMIT, OperationType.ABORT]:
            return f"{optype_to_string[self.op_type]}{self.transaction_id}"
        return f"{optype_to_string[self.op_type]}{self.transaction_id}({self.variable})"

class Schedule():
    """
    A class wrapping the functionality of a Schedule.
    """

    def __init__(self, operations):
        if isinstance(operations,str):
            operations = (parseOperations(operations))
        if not isinstance(operations,list):
            raise ValueError("A Schedule should be initialised with a String or a List of Operations")
        self.operations = operations
        self.num_transactions = max(map(lambda x: x.transaction_id , self.operations))
        self.transactions = [ list(filter(lambda o: o.transaction_id is i, self.operations))
                              for i in range(1, self.num_transactions+1)]
        self.conf = conf(operations)
        self.RF = RF(operations)

    def __repr__(self):
        return ''.join(map(str,self.operations))

    def draw_conflict_graph(self,folder):
        dot = graphviz.Digraph('conflict', comment='The conflict Graph', format='png')
        for i in range(self.num_transactions):
            dot.node(f"T_{i}")
        for p,q in self.conf:
            dot.edge(p,q)
        dot.render(directory=folder, view=False)

    def is_view_serial(self):
        return self.serial() is not None

    # TODO
    def is_conflict_serial(self):
        return False

    # TODO
    def conform(self):
        return ""

    # TODO
    def serial(self):
        """
        Returns the first equivalent serial schedule if the schedule is serializable, else None.
        """
        for perm in permutations(self.transactions):
            SS = Schedule(flatten(perm))
            if SS.RF == self.RF:
                return SS
        return None

    # TODO
    def CS2PL(self):
        """
        Returns a new schedule with locks,
        conforming the the conservative strict two phase locking protocol.
        """
        pass

    # TODO
    def S2PL(self):
        """
        Returns a new schedule with locks,
        conforming the the strict two pahse locking protocol.
        """
        pass

    # TODO
    def C2PL(self):
        """
        Returns a new schedule with locks,
        conforming the the conservative strict two phase locking protocol
        """
        pass

    def __eq__(self,other):
        return self.__repr__() == other.__repr__()

def parseOperations(string):
    """
    Parses the input-string into a list of operations that represents the schedule.
    Input: A valid schedule as string, i.e. "r1(x)w1(x)c1..."
    Output: A python list of Operations
    """
    operations = []
    while string:
        if string[:2] == 'wl' or string[:2] == 'rl':
            operations.append(Operation(int(string[2]),string_to_optype[string[:2]],string[4]))
            string = string[6:]
            continue
        t = string[0]
        if t=='c' or t=='a':
            operations.append(Operation(int(string[1]),string_to_optype[t],""))
            string = string[2:]
        else:
            operations.append(Operation(int(string[1]),string_to_optype[t],string[3]))
            string = string[5:]
    return operations

def conf(oplist):
    return set()


def find_last_write_on(oplist, variable):
    for op in oplist[::-1]:
        if op.variable == variable and op.op_type == OperationType.WRITE:
            return op.transaction_id
    return 0

def RF(oplist):
    rf = set([])
    for i,op in enumerate(oplist):
        if op.op_type == OperationType.READ:
            last_written = find_last_write_on(oplist[:i],op.variable)
            rf.add((last_written,op.variable,op.transaction_id))
    return rf

def flatten(t):
    return [item for sublist in t for item in sublist]
