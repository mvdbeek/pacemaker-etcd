import threading


def call_repeatedly(func, *args):
    stopped = threading.Event()

    def loop():
        while not stopped.wait(15):  # the first call is in `interval` secs
            func(*args)

    t = threading.Thread(target=loop)
    t.daemon = True
    t.start()
    return stopped.set