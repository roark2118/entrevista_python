import argparse
import multiprocessing as mp
import random
import socket
import string
from concurrent.futures import ProcessPoolExecutor

# Constants
VALID_CHARS = string.ascii_letters + string.digits
MIN_LENGTH_CHAIN = 50
MAX_LENGTH_CHAIN = 100
MIN_NUM_SPACES = 3
MAX_NUM_SPACES = 5
BUFFER_SIZE = 1024 * 1024  # 1 MB
CPU_COUNT = mp.cpu_count()
CHUNK_SIZE = 10_000


def generate_chain() -> str:
    chain_length = random.randint(MIN_LENGTH_CHAIN, MAX_LENGTH_CHAIN)
    num_spaces = random.randint(MIN_NUM_SPACES, MAX_NUM_SPACES)
    valid_spaces = []

    # Generate valid space indexes that are not consecutive
    while len(valid_spaces) < num_spaces:
        choice = random.randint(1, chain_length - 1)
        if all(choice + offset not in valid_spaces for offset in (-1, 0, 1)):
            valid_spaces.append(choice)

    chain_chars = random.choices(VALID_CHARS, k=chain_length)
    for pos in valid_spaces:
        chain_chars[pos] = ' '
    
    return ''.join(chain_chars) + "\n"

def generate_chains(num_chains: int):
    return [generate_chain() for _ in range(num_chains)]


def generate_chains_parallel(num_chains: int, client_socket: socket.socket):
    with open('chains.txt', 'w', buffering=BUFFER_SIZE) as f, ProcessPoolExecutor(CPU_COUNT) as pool:
        while num_chains>0:
            if num_chains <= CHUNK_SIZE:
                chains = generate_chains(num_chains)
                f.writelines(chains)
                client_socket.sendall("".join(chains).encode())
                break
            q, r = divmod(num_chains, CHUNK_SIZE)
            if q >= CPU_COUNT:
                chunks = [CHUNK_SIZE] * CPU_COUNT
                num_chains -= CHUNK_SIZE * CPU_COUNT
            else:
                chunks = [CHUNK_SIZE] * q
                if r:
                    chunks.append(r)
                num_chains = 0
            for chains in pool.map(generate_chains, chunks):
                f.writelines(chains)
                client_socket.sendall("".join(chains).encode())
        client_socket.sendall("end".encode())


def receive_from_socket(s: socket.socket):
    with open('results.txt', 'w', buffering=BUFFER_SIZE) as f:
        while True:
            data = s.recv(BUFFER_SIZE)
            if not data:
                return
            results = data.decode()
            if results.endswith("end"):
                f.write(results.removesuffix("end"))
                break
            f.write(results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    DEFAULT_NUM_CHAINS = 1_000_000
    parser.add_argument("-chains", default=DEFAULT_NUM_CHAINS, type=int,
                        help=f"Number of chains to generate (default: {DEFAULT_NUM_CHAINS})")
    
    DEFAULT_ADDRESS = "localhost:3000"
    parser.add_argument("-address", default=DEFAULT_ADDRESS,
                        help=f"Socket address (default: {DEFAULT_ADDRESS})")
    
    args = parser.parse_args()
    
    num_chains: int = args.chains
    if num_chains <= 0:
        raise ValueError("Number of chains should be a positive number.")
    
    address: str = args.address
    try:
        host, port = address.split(":")
        port = int(port)
        address = (host, port)
    except ValueError:
        raise ValueError("Invalid socket address format. Use 'host:port'.")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(address)
        
        reader_process = mp.Process(target=receive_from_socket, args=(client_socket,))
        reader_process.start()
        
        generate_chains_parallel(num_chains, client_socket)
        
        reader_process.join()