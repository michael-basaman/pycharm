import os

base_path = "C:/debian/shred"
small_file = "small.bin"
large_file = "large.bin"

small_path = f"{base_path}/{small_file}"
large_path = f"{base_path}/{large_file}"

bytes_to_write = 1024 * 1024

reps = 1

for count in range(reps):
    if os.path.isfile(small_path):
        os.remove(small_path)

    if os.path.isfile(large_path):
        os.remove(large_path)

    bytes_written = 0

    w = 0
    with open(small_path, 'wb') as f:
        for i in range(1024):
            f.write(os.urandom(bytes_to_write))
            bytes_written = bytes_written + bytes_to_write
            w = w + 1

    try:
        while True:
            if w % 1024 == 0:
                print(f"Bytes written: {bytes_written:,}")

            with open(large_path, 'ab') as f:
                f.write(os.urandom(bytes_to_write))
                bytes_written = bytes_written + bytes_to_write
                w = w + 1

    except Exception as e:
        print(f"Could not write to file: {str(e)}")
        print(f"Totel bytes written: {bytes_written:,}")

    if os.path.isfile(small_path):
        os.remove(small_path)

    if os.path.isfile(large_path):
        os.remove(large_path)

    print("Deleted files")

