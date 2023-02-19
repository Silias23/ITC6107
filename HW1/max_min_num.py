import concurrent.futures


def get_min_max(nums):
    return min(nums), max(nums)


def read_file(i):
    with open(f'f{i}.txt', 'r') as f:
        line = f.readline().split(',')
        line.pop()
    min_num,max_num = get_min_max(line)
    return min_num,max_num


if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Submit tasks to the executor
        results = [executor.submit(read_file, i) for i in range(1, 5)]

    min = 0
    max = 0 
    for f in concurrent.futures.as_completed(results):
        print(f.result())
        min_,max_ = f.result()
        if int(min_ )< min:
            min = int(min_)
        if int(max_ )> max:
            max= int(max_)
    print(f'Min number: {min} Max number {max}')

