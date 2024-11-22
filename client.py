import random
import string
import socket
import argparse
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from threading import Thread
import logging

VALID_CHARS = string.ascii_letters + string.digits
END_SIGNAL = "*".encode()  # Character to signal end of communication in socket


def receive_results(s: socket.socket, buffer_size: int) -> None:
    """Receive results from the server and write them to a file."""
    try:
        with open('results.txt', 'wb', buffering=buffer_size) as f:
            while data := s.recv(buffer_size):
                if data.endswith(END_SIGNAL):
                    f.write(data[:-len(END_SIGNAL)])
                    break
                f.write(data)
    except KeyboardInterrupt:
        pass


def build_chain() -> str:
    """Build a random chain of characters with spaces."""
    chain_length = random.randint(50, 101)
    num_spaces = random.randint(3, 5)

    valid_spaces = []
    while len(valid_spaces) < num_spaces:
        choice = random.randint(1, chain_length - 1)
        if (choice not in valid_spaces
            and choice-1 not in valid_spaces
                and choice+1 not in valid_spaces):
            valid_spaces.append(choice)

    chain_chars = list(random.choices(VALID_CHARS, k=chain_length))
    for pos in valid_spaces:
        chain_chars[pos] = ' '
    return ''.join(chain_chars) + "\n"


def generate_chunk(size: int):
    """Generate a chunk of random character chains."""
    return [build_chain() for _ in range(size)]


def generate_chains_no_parallel(num_chains: int, s: socket.socket, buffer_size: int, chunk_size: int) -> None:
    """Generate chains without parallelism."""
    with open('chains.txt', 'w', buffering=buffer_size) as f:
        def send_lines(lines: list[str]):
            f.writelines(lines)
            s.sendall("".join(lines).encode())

        while num_chains > 0:
            if num_chains <= chunk_size:
                send_lines(generate_chunk(num_chains))
                break
            num_chains -= chunk_size
            send_lines(generate_chunk(chunk_size))
        s.sendall(END_SIGNAL)


def generate_chains(num_chains: int, s: socket.socket, buffer_size: int, chunk_size: int, parallel: int) -> None:
    """Generate chains with optional parallelism."""
    if parallel == 1:
        return generate_chains_no_parallel(num_chains, s, buffer_size, chunk_size)

    with open('chains.txt', 'w', buffering=buffer_size) as f, ProcessPoolExecutor(parallel) as pool:
        def send_lines(lines: list[str]):
            f.writelines(lines)
            s.sendall("".join(lines).encode())

        while num_chains > 0:
            if num_chains <= chunk_size:
                send_lines(generate_chunk(num_chains))
                break

            q, r = divmod(num_chains, chunk_size)
            if q >= parallel:
                chunks = [chunk_size] * parallel
                num_chains -= parallel * chunk_size
            else:
                num_chains = 0
                chunks = [chunk_size] * q
                if r != 0:
                    chunks.append(r)

            for lines in pool.map(generate_chunk, chunks):
                send_lines(lines)

        s.sendall(END_SIGNAL)


def main():
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument("-c", "--chains", default=1_000_000,
                             help=f"number of chains to generate , default 1_000_000",
                             type=int)

    args_parser.add_argument("-a", "--address", default="localhost:3000",
                             help=f"socket address , default localhost:3000")

    args_parser.add_argument("-p", "--parallel", default=1,
                             help=f"parallelism , default no parallelism",
                             type=int)

    args_parser.add_argument("-b", "--buffer", default=1024 * 1024,
                             help=f"buffer size for socket and files in bytes , default 1 MB",
                             type=int)

    args_parser.add_argument("-n", "--chunck", default=10_000,
                             help=f"number of chains to assign to each parallel process , default 10_000",
                             type=int)

    args = args_parser.parse_args()

    num_chains: int = args.chains
    assert num_chains > 0

    buffer_size: int = args.buffer
    assert buffer_size > 0

    parallel: int = args.parallel
    assert parallel > 0

    cpus = mp.cpu_count()
    if parallel > cpus:
        logging.info(f"Parallelism limited to {cpus}")
        parallel = cpus

    chunk_size: int = args.chunck
    assert chunk_size > 0

    try:
        host, port = args.address.split(":")
        port = int(port)
        address = (host, port)
    except ValueError as e:
        raise ValueError(
            "Invalid socket address format. Expected format: host:port") from e
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(address)
        receiver = Thread(target=receive_results, args=[
                          client_socket, buffer_size])
        receiver.start()
        generate_chains(num_chains, client_socket,
                        buffer_size, chunk_size, parallel)
        receiver.join()


if __name__ == "__main__":
    try :
        main()
    except KeyboardInterrupt:
        pass
    
