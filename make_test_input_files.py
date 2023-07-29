"""
Courtesy of ChatGPT, this program will genrate flat files that are ~80% similar
These can be used as inputs to test the main program
"""

import random
import lorem
import os

def create_line(id):
    return f"{id}\t{lorem.sentence()}\t{lorem.sentence()}"

def mutate_line(line):
    id, foo, bar = line.split('\t')
    return f"{id}\t{lorem.sentence()}\t{lorem.sentence()}"

def main():
    
    this_dir = os.path.dirname(os.path.realpath(__file__))
    test_file_dir = os.path.join(this_dir, 'test_files')
    os.makedirs(test_file_dir, exist_ok=True)

    # Create original file
    test_file_1 = os.path.join(test_file_dir, 'test_file1.tsv')
    with open(test_file_1, 'w') as f:
        for id in range(1, 1001):
            f.write(create_line(id) + '\n')

    # Create a second file with 20% lines mutated
    test_file_2 = os.path.join(test_file_dir, 'test_file2.tsv')
    with open(test_file_1, 'r') as orig, open(test_file_2, 'w') as new:
        for line in orig:
            if random.random() < 0.2:  # 20% chance to mutate the line
                new.write(mutate_line(line))
            else:
                new.write(line)

if __name__ == '__main__':
    main()
