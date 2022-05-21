import graphviz
from itertools import permutations
from util import *

class Schedule():
    """
    A class wrapping the functionality of a Schedule.
    """

    def __init__(self, operations):
        if isinstance(operations,str):
            operations = parseOperations(operations)
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

    def adheres_to(self):
        """Returns the Locking Protocol of the schedule as a String"""
        c = is_conservative(self.operations)
        s = is_strict(self.operations)
        if c and s:
            return "CS2PL"
        elif c:
            return "C2PL"
        elif s:
            return "S2PL"
        return "no Protocol"

    def serial(self):
        """
        Returns the first equivalent serial schedule if the schedule is serializable, else None.
        """
        # First, filter out the locks and unlocks
        transactions = list(map(lambda l: list(filter(lambda op: not op.is_lock_or_unlock(),l)),
                                self.transactions.values()))
        # Check if any of the Permutations of the transactions are view equivalent
        for perm in permutations(transactions):
            SS = Schedule(flatten(perm))
            if SS.RF == self.RF:
                return SS
        return None

    def CS2PL(self):
        """
        Returns a new schedule with locks,
        conforming the conservative strict two phase locking protocol.
        """
        oplist = add_protocol(self.operations)
        return Schedule(oplist)

    def S2PL(self):
        """
        Returns a new schedule with locks,
        conforming the strict two phase locking protocol.
        """
        oplist = add_protocol(self.operations, protocol='S2PL')
        return Schedule(oplist)

    def C2PL(self):
        """
        Returns a new schedule with locks,
        conforming the conservative strict two phase locking protocol
        """
        oplist = add_protocol(self.operations, protocol='C2PL')
        return Schedule(oplist)

    def __eq__(self,other):
        return self.__repr__() == other.__repr__()
