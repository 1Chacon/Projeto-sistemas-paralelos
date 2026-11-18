import os
import zmq
def main():
    porta_publicacao = os.getenv("XSUB_PORT", "5557")
    porta_assinaturas = os.getenv("XPUB_PORT", "5558")
    contexto = zmq.Context()
    xsub = contexto.socket(zmq.XSUB)
    xpub = contexto.socket(zmq.XPUB)
    xsub.linger = 0
    xpub.linger = 0
    xsub.bind(f"tcp://*:{porta_publicacao}")
    xpub.bind(f"tcp://*:{porta_assinaturas}")
    print(f"[PROXY PUB/SUB] Pronto | XSUB (servidores): {porta_publicacao} "
          f"| XPUB (bots): {porta_assinaturas}", flush=True)
    try:
        zmq.proxy(xsub, xpub)
    finally:
        xsub.close()
        xpub.close()
        contexto.term()

if __name__ == "__main__":
    main()
