import os
import time
from datetime import datetime, timezone
import msgpack
import zmq

class RelogioLogico:
    def __init__(self):
        self.valor = 0
    def receber(self, pacote):
        self.valor = max(self.valor, int(pacote.get("relogio_logico", 0)))
    def enviar(self):
        self.valor += 1
        return self.valor
class Referencia:
    def __init__(self, timeout=35):
        self.timeout = timeout
        self.relogio = RelogioLogico()
        self.ranks = {}  # ranks fixos 
        self.ativos = {}  # nome -> ultima comunicacao 
    def limpar(self):
        instante = time.monotonic()
        expirados = [nome for nome, ultima in self.ativos.items()
                     if instante - ultima > self.timeout]
        for nome in expirados:
            del self.ativos[nome]
            print(f"[Referencia] SERVIDOR INDISPONIVEL | {nome}", flush=True)
    def processar(self, pedido):
        self.relogio.receber(pedido)
        self.limpar()
        tipo = pedido.get("tipo", "")
        nome = pedido.get("nome", "")
        status = "sucesso"
        resposta = {
            "tipo": tipo + "_resposta", "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "request_id": pedido.get("request_id", ""),
        }
        if tipo == "rank":
            if nome not in self.ranks:
                self.ranks[nome] = len(self.ranks) + 1
            self.ativos[nome] = time.monotonic()
            resposta["rank"] = self.ranks[nome]
            print(f"[Referencia] RANK | {nome}: {resposta['rank']}", flush=True)
        elif tipo == "list":
            resposta["servidores"] = [
                {"nome": n, "rank": self.ranks[n]}
                for n in sorted(self.ativos, key=lambda n: self.ranks[n])
            ]
            print(f"[Referencia] LISTA | {resposta['servidores']}", flush=True)
        elif tipo == "heartbeat":
            if nome not in self.ranks:
                self.ranks[nome] = len(self.ranks) + 1
            self.ativos[nome] = time.monotonic()
            resposta["mensagem"] = "OK"
            print(f"[Referencia] HEARTBEAT OK | {nome} | rank: {self.ranks[nome]}", flush=True)
        else:
            resposta["status"] = "erro"
            resposta["mensagem"] = "Operacao desconhecida"
        resposta["relogio_logico"] = self.relogio.enviar()
        return resposta
def main():
    porta = os.getenv("REF_PORT", "5559")
    timeout = int(os.getenv("HEARTBEAT_TIMEOUT", "35"))
    servico = Referencia(timeout)
    with zmq.Context() as contexto:
        socket = contexto.socket(zmq.REP)
        socket.linger = 0
        socket.bind(f"tcp://*:{porta}")
        print(f"[Referencia] PRONTO | porta: {porta} | expiracao: {timeout}s", flush=True)
        try:
            while True:
                if not socket.poll(1000, zmq.POLLIN):
                    servico.limpar()
                    continue
                pedido = msgpack.unpackb(socket.recv(), raw=False)
                print(f"[Referencia] RECEBIDO <- {pedido}", flush=True)
                resposta = servico.processar(pedido)
                socket.send(msgpack.packb(resposta, use_bin_type=True))
                print(f"[Referencia] ENVIADO -> {resposta}", flush=True)
        finally:
            socket.close()

if __name__ == "__main__":
    main()
