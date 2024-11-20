import argparse
import datetime
import logging
import os
import socket
import time
import multiprocessing as mp

INVALID_CHAIN_WEIGHT = 1000
BUFFER_SIZE = 1024 * 1024

class SocketProcess(mp.Process):
    def __init__(self, queue: mp.Queue, conn_socket: socket.socket, remote_address):
        super().__init__()  # Properly initialize the Process class
        self.logging_queue = queue  
        self.socket = conn_socket
        self.remote_address = remote_address  
        
    def log(self, level: int, message: str) -> None:
        self.logging_queue.put((level, message))

    def get_weight(self, chain: str) -> float:
        if 'aa' in chain.lower():
            self.log(logging.WARNING, f"Double 'a' rule detected >> '{chain.strip()}'")
            return INVALID_CHAIN_WEIGHT
        digits = sum(1 for x in chain if x.isdigit())
        spaces = chain.count(' ')
        letters = len(chain) - digits - spaces
        if spaces == 0:
            self.log(logging.WARNING, f"Chain '{chain}' has no spaces")
            return INVALID_CHAIN_WEIGHT
        return (letters * 1.5 + digits * 2) / spaces
    
    def run(self):
        self.log(logging.INFO, f"Connection from {self.remote_address}")
        start_time = time.perf_counter()
        
        try:
            with self.socket as conn_socket:
                end = False
                while not end:
                    try:
                        data = conn_socket.recv(BUFFER_SIZE)
                        if not data:
                            break

                        chains = data.decode()
                        if chains.endswith("end"):
                            end = True
                            chains = chains.removesuffix("end")

                        results = [f"{chain} : {weight:.2f}" for chain in chains.split("\n")[:-1] 
                                   if (weight := self.get_weight(chain)) != INVALID_CHAIN_WEIGHT]

                        message = "\n".join(results) + "end" if end else "\n".join(results)
                        conn_socket.sendall(message.encode())

                    except (BrokenPipeError, ConnectionResetError):
                        self.log(logging.ERROR, "Connection was closed by the client.")
                        break

                elapsed_time = time.perf_counter() - start_time
                self.log(logging.INFO, f"Process from {self.remote_address} completed in {elapsed_time:.2f} seconds.")

        except Exception as e:
            self.log(logging.ERROR, str(e))


def handle_logging(logging_queue: mp.Queue, log_filename: str):
    if os.path.isfile(log_filename):
        old_file = f'{log_filename}_{datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}'
        os.rename(log_filename, old_file)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_filename, "a")
    fo = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fo)
    logger.addHandler(fh)

    while True:
        record = logging_queue.get()
        if record is None:  # Check for termination signal
            break
        logger.log(*record)


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    DEFAULT_LOG_FILE = "server.log"
    args_parser.add_argument("-logfile", default=DEFAULT_LOG_FILE,
                             help=f"log file name, default {DEFAULT_LOG_FILE}")

    DEFAULT_ADDRESS = "localhost:3000"
    args_parser.add_argument("-address", default=DEFAULT_ADDRESS,
                             help=f"socket address, default {DEFAULT_ADDRESS}")

    args = args_parser.parse_args()
    log_filename = args.logfile

    try:
        host, port = args.address.split(":")
        port = int(port)
        address = (host, port)
    except ValueError as e:
        raise ValueError("Invalid socket address format. Expected format: host:port") from e
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket, mp.Manager() as manager:
        logging_queue = manager.Queue(-1)

        log_listener = mp.Process(target=handle_logging,
                                  args=(logging_queue, log_filename))
        log_listener.start() 
        
        server_socket.bind(address)
        server_socket.listen()

        logging_queue.put((logging.INFO,f"Server started at {datetime.datetime.now()} in {address}"))

        try:
            while True:
                conn_socket, remote_address = server_socket.accept()
                SocketProcess(logging_queue, conn_socket, remote_address).start()  # Pass remote_address
                
        except KeyboardInterrupt:
            logging_queue.put(None)  # Signal to stop logging process
        
        finally:
            log_listener.join()  # Wait for logging process to finish
