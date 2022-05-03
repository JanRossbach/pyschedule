import graphviz

class Operation():
    def __init__(self, transaction):
        self.transaction = transaction


class Transaktion():
    pass


class Schedule():

    def __init__(self,inputstr):
        operations = []
        self.operations = operations

    def __repr__(self):
        return ''.join(self.operations)


class Scheduler():
    def __init__(self,inputstr):
        self.InputSchedule = Schedule(inputstr)
        self.had_locks = 'l' in inputstr

    def __repr__(self):
        return "Hello"

    def draw_conflict_graph(self,folder):
        dot = graphviz.Digraph('conflict', comment='The conflict Graph', format='png')
        dot.node('A')
        dot.node('B')
        dot.edges(['AB'])
        dot.render(directory=folder, view=False)

    def CS2PL(self,):
        pass
