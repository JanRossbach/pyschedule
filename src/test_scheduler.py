import pytest
from scheduler import *

def test_Scheduler_without_locks(capsys):
    schedule = "r1(x)w1(x)r1(y)w1(y)c1r2(z)w2(y)r2(y)c2"
    S = Schedule(schedule)
    assert S.num_transactions == 2
    assert len(S.transactions) == 2
    assert S.transactions[0] and S.transactions[1]
    assert S.RF == set([(0,'x',1),(0,'y',1),(0,'z',2),(2,'y',2)])
    assert S.serial() == S
    assert S.is_view_serial()
    print(S)
    captured = capsys.readouterr()
    assert captured.out == schedule + "\n"


def test_Operation(capsys):
    op = Operation(1, OperationType.READ, "x")
    assert op.variable == "x"
    assert op.transaction_id == 1
    assert op.op_type == OperationType.READ
    print(op)
    captured = capsys.readouterr()
    assert captured.out == "r1(x)\n"

def test_parseOperations():
    assert len(parseOperations("r1(x)w1(x)c1")) == 3
