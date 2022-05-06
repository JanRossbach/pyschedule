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
        self.transactions = { i: list(filter(lambda o: o.transaction_id is i, self.operations))
                              for i in range(1, self.num_transactions+1)}
        self.conf = conf(self.operations)
        self.RF = RF(self.operations)

    def __repr__(self):
        return ''.join(map(str,self.operations))

    def draw_conflict_graph(self,folder):
        dot = graphviz.Digraph('conflict', comment='The conflict Graph', format='png')
        for i in list(self.transactions): # Make a Node T_id for each transaction id
            dot.node(f"T_{i}")
        for p,q in self.conf:
            dot.edge(f"T_{p}",f"T_{q}") # Make an Edge for each conflict pair
        dot.render(directory=folder, view=False)

    def is_view_serial(self):
        return self.serial() is not None

    def is_conflict_serial(self):
        for p,q in self.conf:
            if (q,p) in self.conf:
                return False
        return True

    # TODO
    def conform(self):
        return ""

    def serial(self):
        """
        Returns the first equivalent serial schedule if the schedule is serializable, else None.
        """
        for perm in permutations(self.transactions.values()):
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
        return self

    # TODO
    def S2PL(self):
        """
        Returns a new schedule with locks,
        conforming the the strict two pahse locking protocol.
        """
        return self

    # TODO
    def C2PL(self):
        """
        Returns a new schedule with locks,
        conforming the the conservative strict two phase locking protocol
        """
        return self

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

def conflict(op1,op2):
    """Returns True if the two given Operations are a conflict pair. Else False"""
    if op1.transaction_id == op2.transaction_id:
        return False
    if op1.op_type == OperationType.READ:
        return op2.op_type == OperationType.WRITE
    if op1.op_type == OperationType.WRITE:
        return op2.op_type == OperationType.WRITE or op2.op_type == OperationType.READ
    return False

def aborted(ops, trans_id,):
    """Returns True if the given ops list contains an abort with the given transaction id.
    Else False."""
    for op in ops:
        if op.transaction_id == trans_id and op.op_type == OperationType.ABORT:
            return True
    return False

def conf(oplist):
    """
    Returns the cleaned conflict relation of the schedule.
    Input: A list of Operations
    Output: A set of Tuples (int,int) representing transaction Ids
    """
    crel = set([])
    for i,op1 in enumerate(oplist):
        for op2 in oplist[i+1:]:
            if conflict(op1,op2) and not (aborted(oplist, op1.transaction_id) or aborted(oplist, op2.transaction_id)):
                crel.add((op1.transaction_id,op2.transaction_id))
    return crel


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


d = {1: [1,2,3],2: [3,4,5]}
r = list(d)
