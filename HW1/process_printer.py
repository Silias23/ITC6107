import threading
from time import sleep
import random


def producer(id):
    global nextin
    global buffer
    for i in range(NITEMS): #each producer will create X number of items in the next available space on the buffer.
        empty.acquire()
        lock.acquire()
        buffer[nextin] = (id+1,i)
        print(f'Producer {id+1}: print job {i} created in slot {nextin}')
        nextin = (nextin + 1) % BUFSIZE
        lock.release()
        full.release()
        sleep(0.1 * random.random())


def printer():
    global nextout
    global buffer
    c = 0 #counter used to timeout the process after execution
    time_out_max = 10 #max attempts to check if there are pending jobs
    while True:
        if full._value != 0: #if semaphore is positive => there is at least an item in the queue
            full.acquire()
            lock.acquire()
            item = buffer[nextout]
            print(f'Printer: executed {item} from slot {nextout}')
            nextout = (nextout + 1) % BUFSIZE #cycles through the buffer picking the next item to print // this can be achieved by taking the first item available too
            lock.release()
            empty.release()
            sleep(0.1 * random.random())
        if full._value == 0: #if the 'full' semaphore has reached 0 then no more print jobs are pending. run the cycle again and try to retrieve again IF a generator has created a print job, else shut down
            c += 1 
            print(f'Print queue is empty.. ({c}/{time_out_max})')
            sleep(0.8 * random.random())
            if c >= time_out_max: #if X attempts are made and no new jobs are created, exit
                print('no more print jobs in queue, exiting')
                break


def main():
    #printer thread is initialized first so it is ready to process print requests. 
    #because of that the first message is a print queue empty message because no request producers are initialized yet. 
    printer_thread = threading.Thread(target=printer)
    printer_thread.start()

    # Create process threads
    process_threads = []
    for i in range(TOTAL_PRODUCERS): #create as many threads for producers as in the 
        p = threading.Thread(target=producer,args=[i]) #pass argument the producer id to differentiate in the console
        process_threads.append(p)
        p.start()

    # Wait for all process threads to complete
    for t in process_threads:
        t.join()

    printer_thread.join()




if __name__ == '__main__':
    NITEMS = 30 #number of items each producer will create
    TOTAL_PRODUCERS = 5 #number of threads to create as producers
    BUFSIZE = 10 #lenght of the buffer for print jobs
    buffer = [-1] * BUFSIZE
    nextin = 0
    nextout = 0

    lock = threading.Lock()
    empty = threading.Semaphore(BUFSIZE)
    full = threading.Semaphore(0)

    main()