import argparse
import logging
import logging.handlers
import socket
import time
import multiprocessing as mp


def process_connection(queue: mp.Queue, conn_socket: socket.socket, remote_address: str, buffer_size: int):
    """Handles incoming client connections."""

    INVALID_CHAIN_WEIGHT = 1000
    END_SIGNAL = "*"  # Character to signal end of communication in socket

    logger = setup_logger(queue)
    logger.info(f"Connection from {remote_address}")

    def get_weight(chain: str) -> float:
        """Calculates the weight of the given chain based on specific rules."""

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
        start_time = time.perf_counter_ns()
        end_received = False
        pending = None

        try:
            while not end_received and (data := conn_socket.recv(buffer_size)):
                chains = data.decode().split("\n")
                last = chains[-1]

                # Check if end signal is present
                if last.endswith(END_SIGNAL):
                    last = last.removesuffix(END_SIGNAL)
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

        # Send any pending data before closing connection
        conn_socket.sendall((pending + END_SIGNAL).encode())

        elapsed_time_ms = (time.perf_counter_ns() - start_time) // 1_000_000
        logger.info(f"Process from {remote_address} completed in {elapsed_time_ms} ms")


def setup_logger(queue: mp.Queue):
    """Sets up the logger to use multiprocessing queue."""

    logger = logging.getLogger()
    handler = logging.handlers.QueueHandler(queue)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


def handle_logs(queue: mp.Queue, log_filename: str):
    """Handles logging from the queue to a file."""

    root_logger = logging.getLogger()
    handler = logging.FileHandler(log_filename)
    formatter = logging.Formatter(
        '%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    while (record := queue.get()) is not None:
        try:
            root_logger.handle(record)
        except Exception as e:
            root_logger.error(f"Error handling log record: {e}")


def main():
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument("-l", "--logfile", default="server.log",
                             help=f"log file name, default server.log")

    args_parser.add_argument("-a", "--address", default="localhost:3000",
                             help=f"socket address , default localhost:3000")

    args_parser.add_argument("-b", "--buffer", default=1024 * 1024,
                             help=f"buffer size for socket and files in bytes , default 1 MB",
                             type=int)

    args = args_parser.parse_args()

    buffer_size: int = args.buffer
    assert buffer_size > 0

    log_filename = args.logfile

    try:
        host, port = args.address.split(":")
        port = int(port)
        address = (host, port)
    except ValueError as e:
        raise ValueError(
            "Invalid socket address format. Expected format: host:port") from e

    queue = mp.Queue(-1)

    log_listener = mp.Process(target=handle_logs, args=(queue, log_filename))
    log_listener.start()

    # Setup the main server logger without console output.
    logger = setup_logger(queue)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(address)
        server_socket.listen()
        logger.info(f"server started at {address}")
        try:
            while True:
                conn_socket, remote_address = server_socket.accept()
                mp.Process(target=process_connection, args=[
                           queue, conn_socket, remote_address, buffer_size]).start()

        except KeyboardInterrupt:
            logger.info("Shutting down server...")

        except Exception as e:
            logger.error(f"Server is shutting down because of {e}")

        finally:
            queue.put_nowait(None)   # Signal the log listener to exit
            log_listener.join()      # Wait for the log listener to finish


if __name__ == "__main__":
    main()
