"""
Courtesy of ChatGPT, this program will generate flat files that are ~80% similar
These can be used as inputs to test the main program
"""

import random
import lorem
import os

def create_line(id):
    return f"{id}\t{lorem.sentence()}\t{lorem.sentence()}"

def mutate_line(line):
    id, foo, bar = line.split('\t')
    ret_val =  f"{id}\t{lorem.sentence()}\t{lorem.sentence()}"
    return ret_val

def main():

    rows_to_generate = 10000000  # Remember, some of these will be dropped

    this_dir = os.path.dirname(os.path.realpath(__file__))
    test_file_dir = os.path.join(this_dir, 'test_files')
    os.makedirs(test_file_dir, exist_ok=True)

    # Headers
    headers = "id\tfoo\tbar"

    # Create original file
    test_file_1 = os.path.join(test_file_dir, 'test_file1.tsv')
    with open(test_file_1, 'w') as f:
        f.write(headers + '\n')
        for id in range(1, rows_to_generate + 1):
            f.write(create_line(id) + '\n')

    # Create a second file with 20% lines mutated
    test_file_2 = os.path.join(test_file_dir, 'test_file2.tsv')
    counter = 0
    with open(test_file_1, 'r') as orig, open(test_file_2, 'w') as new:
        for line in orig:
            if random.random() < 0.2 and counter > 0:  # 20% chance to mutate the line
                mutated_line = mutate_line(line)
                if not mutated_line.endswith('\n'):
                    mutated_line += '\n'

                if random.random() < 0.05:  # 20% chance to drop the line
                    pass
                else:
                    new.write(mutated_line)
            else:
                new.write(line)

            counter += 1

    # Go back and randomly drop some lines from the first file
    with open(test_file_1, 'r') as f:
        test_file_str = f.read()
    test_file_lines = test_file_str.split('\n')
    new_lines = []
    counter = 0
    for line in test_file_lines:
        if random.random() < 0.05 and counter > 0:  # 5% chance to drop the line
            pass
        else:
            new_lines.append(line)
        counter += 1
    test_file_str = '\n'.join(new_lines)
    with open(test_file_1, 'w') as f:
        f.write(test_file_str)


if __name__ == '__main__':
    main()
