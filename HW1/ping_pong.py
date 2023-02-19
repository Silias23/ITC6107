import threading

lock = threading.Lock()

def do_ping_pong(ping_pong):
    #acquire a lock and produce either ping or pong depending on the argument passed
    #ping has to end with a space and not new line so that ping pong appears in the same line
    lock.acquire()
    if ping_pong == 'ping':
        print(ping_pong,end= ' ')
    else:
        print(ping_pong)
    lock.release()


def do_job(ping_pong):
    #each thread is assigned to the same function but with different arguments
    #the arguments are passed on the ping_pong function which produces the relevant message
        for i in range(10):
            do_ping_pong(ping_pong)



ping = threading.Thread(target=do_job, args=['ping'])
pong = threading.Thread(target=do_job, args=['pong'])

ping.start()
pong.start()

ping.join()
pong.join()
