import os
import signal
import zmq

def main():
    porta_clientes = int(os.getenv("FRONTEND_PORT", "5555"))
    porta_servidores = int(os.getenv("BACKEND_PORT", "5556"))
    contexto = zmq.Context()
    frontend = contexto.socket(zmq.ROUTER)
    backend = contexto.socket(zmq.DEALER)
    frontend.linger = 0
    backend.linger = 0
    ativo = True
    def encerrar(_sinal, _frame):
        nonlocal ativo
        ativo = False
    signal.signal(signal.SIGTERM, encerrar)
    signal.signal(signal.SIGINT, encerrar)
    try:
        frontend.bind(f"tcp://*:{porta_clientes}")
        backend.bind(f"tcp://*:{porta_servidores}")
        print(f"[BROKER] Iniciado | ROUTER (bots): {porta_clientes} | "
              f"DEALER (servidores): {porta_servidores}", flush=True)
        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)
        poller.register(backend, zmq.POLLIN)
        total_requisicoes = 0
        total_respostas = 0
        while ativo:
            eventos = dict(poller.poll(500))
            if frontend in eventos:
                quadros = frontend.recv_multipart()
                backend.send_multipart(quadros)
                total_requisicoes += 1
                print(f"[BROKER] REQ bot -> servidor | #{total_requisicoes} "
                      f"| quadros={len(quadros)} | bytes={sum(map(len, quadros))}",
                      flush=True)
            if backend in eventos:
                quadros = backend.recv_multipart()
                frontend.send_multipart(quadros)
                total_respostas += 1
                print(f"[BROKER] REP servidor -> bot | #{total_respostas} "
                      f"| quadros={len(quadros)} | bytes={sum(map(len, quadros))}",
                      flush=True)
    finally:
        frontend.close()
        backend.close()
        contexto.term()
        print("[BROKER] Encerrado", flush=True)
        
if __name__ == "__main__":
    main()
