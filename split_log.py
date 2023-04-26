import re

input_file = input("Enter the input file name: ")
output_file_base = input("Enter the output file base name: ")

pattern = r"NodeId\((\d+)\) \/\/"

compiled = re.compile(pattern)
# Open the input file for reading and the output files for writing
with open(input_file, 'r') as input_file, \
        open('output_file_1.txt', 'w') as output_file_1, \
        open('output_file_2.txt', 'w') as output_file_2, \
        open('output_file_3.txt', 'w') as output_file_3, \
        open('output_file_4.txt', 'w') as output_file_4:
    # Define the regular expression pattern to match

    # Read the input file line by line
    for line in input_file:

        # Apply the regular expression to the line
        match = re.search(pattern, line)

        # If the line matches the pattern, get the value of the first group
        if match:
            value = match.group(1)

            if value is not None:
                # Depending on the value, write the line to the appropriate output file
                if value == '0':
                    output_file_1.write(line)
                elif value == '1':
                    output_file_2.write(line)
                elif value == '2':
                    output_file_3.write(line)
                elif value == '3':
                    output_file_4.write(line)

# Close all files
input_file.close()
output_file_1.close()
output_file_2.close()
