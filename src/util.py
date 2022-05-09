from itertools import permutations
from dataclasses import dataclass
from enum import Enum

class OperationType(Enum):
    READ = 1
    WRITE = 2
    READ_LOCK = 3
    WRITE_LOCK = 4
    COMMIT = 5
    ABORT = 6
    UNLOCK = 7

optype_to_string = {OperationType.READ: "r",
                    OperationType.WRITE: "w",
                    OperationType.READ_LOCK: "rl",
                    OperationType.WRITE_LOCK: "wl",
                    OperationType.ABORT: "a",
                    OperationType.COMMIT: "c",
                    OperationType.UNLOCK: "u"}

string_to_optype = {v:k for k,v in optype_to_string.items()}

@dataclass(eq=True,frozen=True)
class Operation():
    """Class for representing single transaction operations"""
    transaction_id: int
    op_type: OperationType
    variable: str

    def __repr__(self):
        if self.op_type in [OperationType.COMMIT, OperationType.ABORT]:
            return f"{optype_to_string[self.op_type]}{self.transaction_id}"
        return f"{optype_to_string[self.op_type]}{self.transaction_id}({self.variable})"

    def is_lock(self):
        return self.op_type in [OperationType.READ_LOCK, OperationType.WRITE_LOCK]

    def is_unlock(self):
        return self.op_type == OperationType.UNLOCK

    def is_lock_or_unlock(self):
        return self.is_lock() or self.is_unlock()

    def is_finish(self):
        return self.op_type in [OperationType.ABORT, OperationType.COMMIT]

    def equiv_write(self):
        return Operation(self.transaction_id, OperationType.WRITE,self.variable)

    def equiv_read(self):
        return Operation(self.transaction_id, OperationType.READ,self.variable)

    def equiv_write_lock(self):
        return Operation(self.transaction_id, OperationType.WRITE_LOCK,self.variable)

    def equiv_read_lock(self):
        return Operation(self.transaction_id, OperationType.READ_LOCK,self.variable)

    def equiv_unlock(self):
        return Operation(self.transaction_id, OperationType.UNLOCK,self.variable)

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
    if op1.transaction_id == op2.transaction_id or op1.variable != op2.variable:
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

def conservative(oplist):
    """
    Returns True if the given oplist uses conservative locking,
    meaning all locks happen together at the start of each transaction.
    """
    for i,op1 in enumerate(oplist):
        for op2 in oplist[i+1:]:
            if op1.transaction_id == op2.transaction_id and (not op1.is_lock() and op2.is_lock()): # A non-lock Operation happens before a lock Operation of the same Transaction
                return False
    return True

def strict(oplist):
    """
    Returns True if the given oplist uses conservative locking,
    meaning all the unlocks happen together at the end of each transaction.
    """
    for i,op1 in enumerate(oplist):
        for op2 in oplist[i+1:]:
            if op1.transaction_id == op2.transaction_id and (op1.is_unlock() and not (op2.is_unlock() or op2.is_finish())): # A unlock Operation happens before a non-unlock Operation of the same Transaction.
                return False
    return True

def add_before_start(operations, new_ops, tid):
    """
    Adds the new_ops to the given operations List
    before the start of the Transaction with id tid.
    """
    i = -1
    for j,op in enumerate(operations):
        if op.transaction_id == tid:
            i = j
            break
    return operations[:i] + new_ops + operations[i:]

def add_before_commit(operations, new_ops, tid):
    """
    Adds the new_ops to the given operations List
    before the commit of the Transaction with id tid.
    """
    i = -1
    for j,op in enumerate(operations):
        if op.transaction_id == tid and op.op_type == OperationType.COMMIT:
            i = j
            break
    return operations[:i] + new_ops + operations[i:]

def add_locks_when_needed(operations, tid):
    n = 0 # Number of locks added
    oplist = operations.copy()
    for i,op in enumerate(operations):
        if op.transaction_id != tid:
            continue
        j = i+n # index of the operation in oplist
        if op.op_type == OperationType.READ:
            rl = op.equiv_read_lock()
            wl = op.equiv_write_lock()
            if rl not in oplist[:j] and wl not in oplist[:j] and op.equiv_write() not in oplist[j+1:]:
                oplist = oplist[:j] + [rl] + oplist[j:]
                n+=1
            else:
                oplist = oplist[:j] + [wl] + oplist[j:]
                n+=1
        if op.op_type == OperationType.WRITE:
            wl = op.equiv_write_lock()
            if wl not in oplist[:j]:
                oplist = oplist[:j] + [wl] + oplist[j:]
                n+=1
    return oplist

def add_unlocks_when_needed(operations, tid):
    n = 0 # Number of locks added
    oplist = operations.copy()
    for i,op in enumerate(operations):
        if op.transaction_id != tid:
            continue
        j = i + n
        ul = op.equiv_unlock()
        w = op.equiv_write()
        r = op.equiv_read()
        if w not in operations[i+1:] and r not in operations[i+1:]:
            oplist = oplist[:j+1] + [ul] + oplist[j+1:]
            n+=1
    return oplist

def needed_locks(transaction_operations):
    """
    Given a list of operations of a transaction,
    returns a Tuple of the set of needed locks and needed unlocks.
    """
    needed_locks = set([])
    needed_unlocks = set([])
    for op in transaction_operations:
        wl = op.equiv_write_lock()
        rl = op.equiv_read_lock()
        u = op.equiv_unlock()
        if op.op_type == OperationType.WRITE:
            needed_locks = needed_locks - set([rl]) # Remove Read-locks if present
            needed_locks.add(wl)
            needed_unlocks.add(u)
        if op.op_type == OperationType.READ and wl not in needed_locks:
            needed_locks.add(rl)
            needed_unlocks.add(u)
    return needed_locks, needed_unlocks

def add_protocol(ops, protocol='CS2PL'):
    """
    Given a list of operations, this function cleans the present locks and
    returns a new list with locks according to the specified Protocol.
    Valid Values for protocol are "CS2PL", "S2PL" and "C2PL"
    """
    if protocol not in ['CS2PL', 'S2PL', 'C2PL']:
        raise ValueError("Please specify a valid protocol.")
    oplist = ops.copy()
    oplist = list(filter(lambda op: not op.is_lock_or_unlock(), oplist))
    transactions = range(1,max(list(map(lambda op: op.transaction_id,oplist))))
    for i in transactions:
        transaction_operations = list(filter(lambda op: op.transaction_id == i, oplist))
        locks, unlocks = needed_locks(transaction_operations)
        if protocol == 'CS2PL':
            oplist = add_before_start(oplist,list(locks),i)
            oplist = add_before_commit(oplist,list(unlocks),i)
        elif protocol == 'C2PL':
            oplist = add_before_start(oplist,list(locks),i)
            oplist = add_unlocks_when_needed(oplist,i)
        else:
            oplist = add_locks_when_needed(oplist,i)
            oplist = add_before_commit(oplist,list(unlocks),i)
    return oplist
