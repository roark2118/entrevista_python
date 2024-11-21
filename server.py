import argparse
import datetime
import logging
import logging.handlers
import os
import socket
import time
import multiprocessing as mp

INVALID_CHAIN_WEIGHT = 1000
# character to signal end of comunication in socket
END = "*"

def process_connection(queue: mp.Queue, conn_socket: socket.socket, remote_address: str, buffer_size: int):
    logger = setup_logger(queue)
    logger.info(f"Connection from {remote_address}")

    def get_weight(chain: str) -> float:
        if 'aa' in chain.lower():
            logger.warning(f"Double 'a' rule detected >> '{chain.strip()}'")
            return INVALID_CHAIN_WEIGHT
        digits = sum(1 for x in chain if x.isdigit())
        spaces = chain.count(' ')
        letters = len(chain) - digits - spaces
        if spaces == 0:
            logger.warning(f"Chain '{chain}' has no spaces")
            return INVALID_CHAIN_WEIGHT
        return (letters * 1.5 + digits * 2) / spaces

    with conn_socket:
        start_time = time.perf_counter()
        end_received = False
        pending = None
        try:
            while not end_received and (data := conn_socket.recv(buffer_size)):
                chains = data.decode().split("\n")
                last = chains[-1]
                if last.endswith(END):
                    last = last.removesuffix(END)
                    if last:
                        chains[-1] = last
                    else:
                        chains.pop()
                    end_received = True
                if pending:
                    chains[0] = pending + chains[0]
                pending = last    
                results = [f"{chain} : {weight:.2f}" for chain in chains 
                            if (weight := get_weight(chain)) != INVALID_CHAIN_WEIGHT]
                conn_socket.sendall("\n".join(results).encode())    
        except BrokenPipeError as e:
            logger.error(f"Error while handling {remote_address}: {e}")
        except KeyboardInterrupt:
            return
        conn_socket.sendall((pending + END).encode())
        elapsed_time = time.perf_counter() - start_time
        logger.info(f"Process from {remote_address} completed in {elapsed_time:.2f} seconds.")

def setup_logger(queue: mp.Queue):
    logger = logging.getLogger() 
    logger.setLevel(logging.INFO)
    handler = logging.handlers.QueueHandler(queue)
    logger.addHandler(handler)
    
    return logger

def handle_logs(queue: mp.Queue, log_filename: str):
    # Check if log file exists for rotation
    if os.path.isfile(log_filename):
        old_file = f'{log_filename}_{datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}'
        os.rename(log_filename, old_file)

    # Create file handler for logging
    root_logger = logging.getLogger()  
    handler = logging.FileHandler(log_filename)
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


    while (record := queue.get()) is not None:
        try:
            root_logger.handle(record)
        except Exception as e:
            root_logger.error(f"Error handling log record: {e}")


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument("-l", default="server.log",
                             help=f"log file name, default server.log")

    args_parser.add_argument("-a",default="localhost:3000",
                            help=f"socket address , default localhost:3000")


    args_parser.add_argument("-b",
                            default= 1024 * 1024,
                            help=f"buffer size for socket and files in bytes , default 1 MB",
                            type=int)

    args = args_parser.parse_args()

    buffer_size : int = args.b
    assert buffer_size > 0

    log_filename = args.l

    try:
        host, port = args.a.split(":")
        port = int(port)
        address = (host, port)
    except ValueError as e:
        raise ValueError("Invalid socket address format. Expected format: host:port") from e

    queue = mp.Queue(-1)
  
    log_listener = mp.Process(target=handle_logs, args=(queue, log_filename))
    log_listener.start()
    
    # Setup the main server logger without console output.
    logger = setup_logger(queue)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(address)
        server_socket.listen()   
        try:
            while True:
                conn_socket, remote_address = server_socket.accept()
                mp.Process(target=process_connection,args=[queue, conn_socket, remote_address, buffer_size]).start()     
        except KeyboardInterrupt:
            logger.info("Shutting down server...")
        except Exception as e:
            logger.info(f"Server is shutting down because of {e}")  
        finally:
            queue.put_nowait(None)   #Signal the log listener to exit
            log_listener.join()      #Wait for the log listener to finish
