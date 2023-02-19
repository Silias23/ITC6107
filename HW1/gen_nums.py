import random
import concurrent.futures

def create_file(i):
    #create a txt file that contains X amount of comma delimited numbers
    nums = 1048576
    rng = 999999999
    with open(f'f{i}.txt', 'w') as f:
        for i in range(nums):
            line = str(random.randint(rng*-1,rng)) + ',' # each number is generated at random, between -999999999 and 999999999. to simplify, no decimal numbers are used
            f.write(line)
    f.close()


with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    #create an executor for each file and each file will be created at the same time
    futures = [executor.submit(create_file, i) for i in range(1, 5)]



