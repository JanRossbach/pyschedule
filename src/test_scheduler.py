import pytest
from scheduler import *
from util import *

def test_Operation(capsys):
    op = Operation(1, OperationType.READ, "x")
    print(op)
    captured = capsys.readouterr()
    assert op.variable == "x"
    assert op.transaction_id == 1
    assert op.op_type == OperationType.READ
    assert captured.out == "r1(x)\n"

def test_parseOperations():
    assert len(parseOperations("r1(x)w1(x)c1")) == 3
    assert parseOperations("u1(x)")[0].op_type == OperationType.UNLOCK

def test_Schedule_without_locks(capsys):
    schedule = "r1(x)w1(x)r1(y)w1(y)c1r2(z)w2(y)r2(y)c2"
    S = Schedule(schedule)
    print(S)
    captured = capsys.readouterr()
    assert S.num_transactions == 2
    assert len(S.transactions) == 2
    assert S.transactions[1] and S.transactions[2]
    assert S.RF == set([(0,'x',1),(0,'y',1),(0,'z',2),(2,'y',2)])
    assert S.conf == set([(1,2)])
    assert S.serial() == S
    assert S.is_view_serial()
    assert S.is_conflict_serial()
    assert captured.out == schedule + "\n"

def test_Schedule_with_locks(capsys):
    schedule = "wl1(x)wl1(y)r1(x)w1(x)u1(x)r1(y)w1(y)u1(y)c1rl2(z)wl2(y)r2(z)u2(z)w2(y)r2(y)u2(y)c2"
    S = Schedule(schedule)
    print(S)
    captured = capsys.readouterr()
    assert S.num_transactions == 2
    assert S.adheres_to() == "C2PL"
    assert S.is_view_serial()
    assert S.is_conflict_serial()
    assert S.conf == set([(1,2)])
    assert S.serial() == Schedule("r1(x)w1(x)r1(y)w1(y)c1r2(z)w2(y)r2(y)c2")
    assert S.RF == set([(0,'x',1),(0,'y',1),(0,'z',2),(2,'y',2)])
    assert captured.out == "wl1(x)wl1(y)r1(x)w1(x)u1(x)r1(y)w1(y)u1(y)c1rl2(z)wl2(y)r2(z)u2(z)w2(y)r2(y)u2(y)c2\n"

def test_adheres_to():
    schedule1 = "rl1(x)r1(x)wl1(x)w1(x)rl1(y)r1(y)wl1(y)w1(y)u1(x)u1(y)c1rl2(z)r2(z)wl2(y)w2(y)r2(y)u2(z)u2(y)c2"
    schedule2 = "wl1(x)wl1(y)r1(x)w1(x)u1(x)r1(y)w1(y)u1(y)c1rl2(z)wl2(y)r2(z)u2(z)w2(y)r2(y)u2(y)c2"
    S = Schedule(schedule1)
    S2 = Schedule(schedule2)
    assert S2.adheres_to() == "C2PL"
    assert S.adheres_to() == "S2PL"

def test_needed_locks():
    schedule = "r1(x)w1(x)r1(y)w1(y)c1r2(z)w2(y)r2(y)c2"
    S = Schedule(schedule)
    oplist = S.operations
    locks, unlocks = needed_locks(oplist)
    assert len(locks) == 4
    assert len(unlocks) == 4

def test_locking_protocols():
    schedule = "r1(x)w1(x)r1(y)w1(y)c1r2(z)w2(y)r2(y)c2"
    S = Schedule(schedule)
    print(S.S2PL())
    assert S.CS2PL().adheres_to() == 'CS2PL'
    assert S.S2PL().adheres_to() == 'S2PL'
    assert S.C2PL().adheres_to() == 'C2PL'

def test_schedule_not_serializable():
    schedule = ""
    S = Schedule(schedule)
    assert not S.is_view_serial()
    assert not S.is_conflict_serial()
    assert S.serial() == None
    assert S.conf == set([(1,3),(2,3),(3,1),(2,1)])
