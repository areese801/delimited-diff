# Delim Diff Tool

This Python program is a diffing tool designed to compare two delimited files based on a composite key. It provides statistics and detailed differences between the two files, helping users identify discrepancies and commonalities between datasets.

## Installation

To use the Delim Diff Tool, follow these steps:

1. Clone the repository to your local machine.
2. Install the required dependencies using pip:
   ```
   pip install csv
   pip install io
   pip install argparse
   ```
    - Alternatively, you can install the dependencies by calling the `make_env.sh` script, but YMMV.  This will create a virtual environment under `venv` and install the dependencies listed in `requirements.txt`:
   ```
   bash make_env.sh $(pwd) -f
   ``` 
3. Run the program by executing the main script:
   ```
   python delim_diff.py --file-a path/to/fileA.txt --file-b path/to/fileB.txt
   ```

## Usage

The tool accepts the following command-line arguments:

- `--file-a`, `-a`: The path to the first delimited file (File A) for comparison. (Required)
- `--file-b`, `-b`: The path to the second delimited file (File B) for comparison. (Required)
- `--delimiter`, `-d`: The delimiter used in both files for parsing. (Optional, default: tab `\t`)
- `--composite-key-fields`, `-k`: A list of fields to use as the composite key. If not specified, the first matched field (from left to right) will be used.
- `--unimportant-fields`, `-u`: A list of fields to ignore when comparing the files.
- `--verbose`, `-v`: Controls the verbosity of the program.

## Examples

1. Basic usage:
   ```
   python delim_diff.py --file-a data/fileA.txt --file-b data/fileB.txt
   ```

2. Using a custom delimiter (e.g., comma `,`):
   ```
   python delim_diff.py --file-a data/fileA.csv --file-b data/fileB.csv --delimiter ,
   ```

3. Specifying composite key fields:
   ```
   python delim_diff.py --file-a data/fileA.txt --file-b data/fileB.txt --composite-key-fields ID Name
   ```

4. Ignoring unimportant fields:
   ```
   python delim_diff.py --file-a data/fileA.txt --file-b data/fileB.txt --unimportant-fields Description
   ```

## Notes

- The Delim Diff Tool expects both input files (File A and File B) to be real files. If either file does not exist, an error will be raised.
- If the delimiter is not specified, the tool will infer it from the files. If the inferred delimiters from both files differ, the tool will fail and prompt the user to specify a delimiter explicitly.
- The composite key fields are used to uniquely identify records during the comparison process. If not specified, the tool will use the first matched field (from left to right) as the key.  For example, if both files have a field called `ID`, in the leftmost column the tool will use that field as the key.
- Unimportant fields are ignored during the comparison. If an unimportant field is part of the composite key, it will raise an error.
- The tool provides detailed statistics and differences between the files, including the number of matched fields, unmatched fields, total lines with differences, total field-level differences, and rows present in one file but not the other.  These are printed to `stdout`
- The tool also returns a JSON object describing the differences between the files.  This object can be used by other programs to perform additional processing or analysis.

## Contributing

Contributions to the Delim Diff Tool are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request on the GitHub repository.