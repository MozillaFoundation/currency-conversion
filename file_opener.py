def file_opener(file, mode = 'r'):
    if file:
        open_file = open(file)
        read_file = open_file.read()
        open_file.close()
    return read_file