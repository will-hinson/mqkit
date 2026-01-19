from mqkit.workers.threaded import MonotonicCounter


def test_monotonic_counter_sequential() -> None:
    counter = MonotonicCounter(start=10)
    assert counter.next() == 10
    assert counter.next() == 11
    assert counter.next() == 12
    assert counter.next() == 13


def test_monotonic_counter_thread_safety() -> None:
    import threading

    counter = MonotonicCounter(start=0)
    results = []
    lock = threading.Lock()

    def worker():
        for _ in range(1000):
            value = counter.next()
            with lock:
                results.append(value)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 5000
    assert sorted(results) == list(range(5000))
