import concurrent.futures
import math
import time
import pandas as pd

def is_prime(num):
    #simple prime check script
    if num < 2:
        return False
    for i in range(2, int(math.sqrt(num)) + 1):
        if num % i == 0:
            return False
    return True

def count_primes(start_end):
    #each process will check for primes in the specific number range it was given
    #for every number in the range check if it is prime and return the # of primes found
    start, end = start_end
    count = 0
    for num in range(start, end):
        if is_prime(num):
            count += 1
    return count

def create_ranges(chunk,processes):
    #creates the range of numbers for each process to execute
    #if the numbers to process are 0-1000 between 4 processors, then the chunks created are
    # [(0,250),(250,500),(500,750),(750,1000)]
    ranges = []
    for i in range(processes):
        ranges.append((i * chunk, (i+1) * chunk))
    return ranges

if __name__ == '__main__':
    num_list = [1000, 10000, 100000, 1000000, 10000000]
    processes = [4, 8, 16]
    df = pd.DataFrame(columns=['Number', 'Processes', 'Time'])
    primes_found = []
    for num_processes in processes:
        for max_num in num_list:
            #split the numbers equally among the number of processes - inneficient because of prime number distribution
            chunk_size = max_num // num_processes
            #create ranges so each process will find primes in a given range
            ranges = create_ranges(chunk_size, num_processes)
            start_time = time.time()

            with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
                results = executor.map(count_primes, ranges)

            end_time = time.time()
            
            #used for displaying the primes found for each number range 
            #uses only the first run to retrieve the prime number count since this is a repeating process
            if num_processes == 4 :
                primes_found.append(sum(results))
            #print(f"Found {sum(results)} primes less than {max_num} in {end_time - start_time:.2f} seconds using {num_processes} processes.")
            total_time = end_time- start_time
            #append results of each number/process exetution to a df
            df = df.append({'Number':max_num, 'Processes':num_processes, 'Time':total_time}, ignore_index = True)
    #group results and use the numbers as index 
    df = df.pivot(index='Number', columns='Processes', values='Time')
    df.columns.name = 'Processes'
    df['Primes Found'] = primes_found
    print(df)


