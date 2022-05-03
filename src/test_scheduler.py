import pytest
from scheduler import Scheduler

def test_Scheduler_without_locks():
    S = Scheduler("r1(x)w1(x)r1(y)w1(y)c1r2(z)w2(y)r2(y)c2")
    assert S.had_locks == False
