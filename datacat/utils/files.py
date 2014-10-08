def file_copy(src, dest, blocksize=4096):
    """
    Copy data between two file descriptors or file-like objects.

    :param src:
        An object with a ``.read(size)`` method
    :param dest:
        An object with a ``.write(data)`` method
    :param blocksize:
        The size of blocks read from src and written to dest.
    """
    while True:
        data = src.read(blocksize)
        if not data:
            return
        dest.write(data)


def file_read_chunks(src, blocksize=4096):
    """
    Read a file in chunks

    :param src:
        An object with a ``.read(size)`` method
    :param blocksize:
        The size of chunks in which to read the file / stream
    """
    while True:
        data = src.read(blocksize)
        if not data:
            return
        yield data
