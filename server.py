import logging
import socket
import time
import datetime
import re
import os
import argparse
from threading import Thread


INVALID_CHAIN_WEIGHT = 1000
BUFFER_SIZE = 1024

# def calculate_chain_weight(chain: str) -> float:
#     if 'aa' in chain.lower(): 
#         logging.warning(f"Double 'a' rule detected >> '{chain.strip()}'")
#         return INVALID_CHAIN_WEIGHT
#     digits = sum(1 for x in chain if x.isdigit() )
#     spaces = chain.count(' ')
#     letters = len(chain) - digits - spaces
#     if spaces == 0 :
#         logging.warning(f"chain {chain} has no spaces")
#         return INVALID_CHAIN_WEIGHT
#     return (letters * 1.5 + digits * 2) / spaces

def calculate_chain_weight(chain: str) -> float:
    digits = spaces =  0
    for i,c in enumerate(chain):
        if i>0 and c.lower()=='a' and chain[i-1].lower()=='a':
            logging.warning(f"Double 'a' rule detected >> '{chain}'")
            return INVALID_CHAIN_WEIGHT
        if c == ' ':
            spaces+=1
        elif '0' <= c <= '9':
            digits += 1
    if spaces == 0:
        logging.warning(f"chain {chain} has no spaces")
        return INVALID_CHAIN_WEIGHT
    letters = len(chain)-digits-spaces
    return (letters * 1.5 + digits * 2) / spaces



def process_connection(conn: socket.socket,addr : str) -> None:
    try:
        with conn:
            logging.info(f"Connected by {addr}")
            start_time = time.perf_counter()
            while True:
                data = conn.recv(BUFFER_SIZE)
                if not data:
                    break
                chain = data.decode('utf-8').strip()
                weight = calculate_chain_weight(chain)
                response = f"{chain} : {weight:.2f}\n" if weight != INVALID_CHAIN_WEIGHT else "0"
                conn.sendall(response.encode('utf-8'))
            elapsed_time = time.perf_counter() - start_time
            print(elapsed_time)
            logging.info(f"Process from {addr} completed in {elapsed_time:.2f} seconds.")
    except ValueError as e:
        logging.error(e)

def setup_logging(logfile_name: str) -> None:
    """Setup logging configuration."""
    if os.path.isfile(logfile_name):
        old_file = f'{logfile_name}_{datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}'
        os.rename(logfile_name, old_file)
    
    logging.basicConfig(filename=logfile_name, level=logging.INFO,
                        format='%(asctime)s %(levelname)s:%(message)s')

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    DEFAULT_LOG_FILE = "server.log"
    args_parser.add_argument("-logfile", default=DEFAULT_LOG_FILE,
                             help=f"log file name, default {DEFAULT_LOG_FILE}")
    
    DEFAULT_ADDRESS = "localhost:3000"
    args_parser.add_argument("-address", default=DEFAULT_ADDRESS,
                             help=f"socket address, default {DEFAULT_ADDRESS}")
    
    args = args_parser.parse_args()
    setup_logging(args.logfile)
    # Parse socket address
    try:
        host, port = args.address.split(":")
        port = int(port)
        address = (host, port)
    except ValueError as e:
        raise ValueError("Invalid socket address format. Expected format: host:port") from e
    

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(address)
        server_socket.listen()
        
        now = datetime.datetime.now()
        logging.info(f"Server started at {now} in {address}")
        
        try:
            while True:
                conn, addr = server_socket.accept()
                process = Thread(target=process_connection, args=(conn, addr))
                process.start()
        
        except KeyboardInterrupt:
            logging.info("Server is shutting down.")
