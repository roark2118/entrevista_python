import random
import string
import socket
import time
import argparse
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import threading


VALID_CHARS = string.ascii_letters + string.digits
MIN_LENGTH_CHAIN = 50
MAX_LENGTH_CHAIN = 100
MIN_NUM_SPACES = 3
MAX_NUM_SPACES = 5

def generate_chain() -> str:
    chain_length = random.randint(MIN_LENGTH_CHAIN, MAX_LENGTH_CHAIN)
    num_spaces = random.randint(MIN_NUM_SPACES, MAX_NUM_SPACES)
    valid_spaces = []
    #generate valid space indexes that are not consecutives 
    #and are beetwen 1 and chain_length - 1
    for _ in range(num_spaces):
        while True:
            choice = random.randint(1, chain_length - 1)
            if (choice not in valid_spaces 
            and choice-1 not in valid_spaces 
            and choice+1 not in valid_spaces):
                valid_spaces.append(choice)
                break

    chain_chars = list(random.choices(VALID_CHARS, k=chain_length))
    for pos in valid_spaces:
        chain_chars[pos] = ' '
    return ''.join(chain_chars)+"\n"

def generate_chains(num_chains: int):
    return [generate_chain() for _ in range(num_chains)]

def generate_chains_parallel(num_chains: int):
    cpu_count = multiprocessing.cpu_count()//2
    chunck_size = num_chains  // cpu_count
    q,r = divmod(num_chains,chunck_size)
    chunks = [ chunck_size ] * q
    if r:
        chunks.append(r)
    with open('chains.txt', 'w', buffering=4096) as f:
        with ProcessPoolExecutor(cpu_count) as exec:
            for lines in exec.map(generate_chains,chunks):
                f.writelines(lines)      

# def receive(s : socket.socket):
#     with open('results.txt', 'w',buffering=2048) as result_file:
#         response = s.recv(1024).decode('utf-8')    
#         if response != "0":
#             result_file.write(response)

def send_chains_to_server(address):
    with open('chains.txt', 'r') as chains_file:
        with open('results.txt', 'w',buffering=2048) as result_file:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(address)
                while line := chains_file.readline().strip():
                    s.sendall(line.encode('utf-8'))
                    response = s.recv(1024).decode('utf-8')    
                    if response != "0":
                        result_file.write(response)

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    DEFAULT_NUM_CHAINS = 1_000_000
    args_parser.add_argument("-chains",
                            default=DEFAULT_NUM_CHAINS,
                            help=f"number of chains to generate , at least 100 , default {DEFAULT_NUM_CHAINS}",
                            type=int)
    DEFAULT_ADDRESS = "localhost:3000"
    args_parser.add_argument("-address",
                            default=DEFAULT_ADDRESS,
                            help=f"socket address , default {DEFAULT_ADDRESS}")
    args = args_parser.parse_args()
    num_chains: int = args.chains
    if num_chains<=0:
        raise ValueError("number of chains should be at positive number")
    address: str = args.address
    try:
        host,port = address.split(":")
        port= int(port)
        address = (host,port)
    except :
        raise ValueError("invalid socket address")
    start = time.perf_counter()
    generate_chains_parallel(num_chains)
    send_chains_to_server(address)
    print(f"take {time.perf_counter()-start} s")
    