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


@call_repeatedly
def send_alive_signal(client, prefix, ip, ttl):
    client.refresh("%s/nodes/%s" % (prefix, ip), ttl=ttl)
