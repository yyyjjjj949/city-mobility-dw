"""One Spark driver at a time on this local workstation."""
from contextlib import contextmanager
from functools import wraps
from pathlib import Path


@contextmanager
def compute_lock(path=None):
    import msvcrt
    path=Path(path) if path is not None else Path(__file__).resolve().parents[1]/'work/spark-compute.lock'
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as handle:
        handle.seek(0);handle.write(b'0');handle.flush();handle.seek(0)
        try:msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        except OSError as exc:raise RuntimeError('Spark is busy with another project job; retry after it stops') from exc
        try:yield
        finally:
            handle.seek(0);msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)


def exclusive_compute(fn):
    @wraps(fn)
    def wrapped(*args,**kwargs):
        with compute_lock():return fn(*args,**kwargs)
    return wrapped
