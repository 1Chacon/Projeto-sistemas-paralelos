import json
import os
import queue
import re
import threading
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import msgpack
import zmq

class RelogioLogico:
    def __init__(self):
        self.valor = 0
        self.lock = threading.Lock()
    def receber(self, pacote):
        with self.lock:
            self.valor = max(self.valor, int(pacote.get("relogio_logico", 0)))
    def enviar(self):
        with self.lock:
            self.valor += 1
            return self.valor 
RELOGIO = RelogioLogico()
DESVIO_RELOGIO = 0.0  
TRAVA_HORA = threading.Lock()

def hora_aplicacao():
    with TRAVA_HORA:
        return time.time() + DESVIO_RELOGIO
def definir_desvio(desvio):
    global DESVIO_RELOGIO
    with TRAVA_HORA:
        DESVIO_RELOGIO = float(desvio)
def agora():
    return datetime.fromtimestamp(hora_aplicacao(), timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")

def campos_legiveis(dados):
    nomes = {"usuario": "nome", "timestamp": "horario", "request_id": "id",
             "status": "resultado"}
    return " | ".join(f"{nomes.get(chave, chave)}: {valor}" for chave, valor in dados.items())
def rotulo_servidor(identificador):
    if identificador.startswith("servidor") and identificador[8:].isdigit():
        return f"Servidor {identificador[8:]}"
    return identificador
def criar_tabelas(conexao):
    conexao.executescript("""
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT NOT NULL,
            timestamp_login TEXT NOT NULL,
            timestamp_recebimento TEXT NOT NULL,
            request_id TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS canais (
            nome TEXT PRIMARY KEY,
            criado_por TEXT NOT NULL,
            timestamp_criacao TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS publicacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL UNIQUE,
            usuario TEXT NOT NULL,
            canal TEXT NOT NULL,
            mensagem TEXT NOT NULL,
            timestamp_envio TEXT NOT NULL,
            timestamp_servidor TEXT NOT NULL,
            conteudo BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origem TEXT NOT NULL,
            tipo TEXT NOT NULL,
            usuario TEXT,
            canal TEXT,
            timestamp_envio TEXT NOT NULL,
            registrado_em TEXT NOT NULL,
            conteudo BLOB NOT NULL
        );
    """)
def registrar_evento(conexao, origem, tipo, usuario, canal, timestamp, conteudo):
    conexao.execute(
        "INSERT INTO eventos (origem, tipo, usuario, canal, timestamp_envio, registrado_em, conteudo) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (origem, tipo, usuario, canal, timestamp, agora(), conteudo),
    )
def resposta(tipo, requisicao, status="sucesso", mensagem=""):
    return {
        "tipo": tipo,
        "usuario": requisicao.get("usuario", ""),
        "request_id": requisicao.get("request_id", ""),
        "timestamp": agora(),
        "status": status,
        "mensagem": mensagem,
        "servidor": os.getenv("SERVER_ID", "servidor1"),
        "relogio_logico": RELOGIO.enviar(),
    }
def criar_canal(conexao, canal, usuario, timestamp):
    cursor = conexao.execute(
        "INSERT OR IGNORE INTO canais (nome, criado_por, timestamp_criacao) VALUES (?, ?, ?)",
        (canal, usuario, timestamp),
    )
    return cursor.rowcount == 1
def processar(requisicao, conexao, pub, identificador, imprimir=True, registrar_mensagem=None):
    tipo = requisicao.get("tipo", "")
    usuario = requisicao.get("usuario", "")
    canal = requisicao.get("canal", "")
    timestamp = requisicao.get("timestamp", "")
    request_id = requisicao.get("request_id", "")
    rotulo = rotulo_servidor(identificador)
    if tipo == "login":
        conexao.execute(
            "INSERT OR IGNORE INTO logins "
            "(usuario, timestamp_login, timestamp_recebimento, request_id) VALUES (?, ?, ?, ?)",
            (usuario, timestamp, agora(), request_id),
        )
        if imprimir:
            print(f"[{rotulo}] LOGIN OK | nome: {usuario}", flush=True)
        return resposta("login_resposta", requisicao, mensagem="Login registrado")
    if tipo == "listar_canais":
        canais = [linha[0] for linha in conexao.execute("SELECT nome FROM canais ORDER BY nome")]
        retorno = resposta("listar_canais_resposta", requisicao, mensagem="Canais listados")
        retorno["canais"] = canais
        if imprimir:
            print(f"[{rotulo}] LISTAR CANAIS | {canais}", flush=True)
        return retorno
    if tipo == "criar_canal":
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}", canal):
            retorno = resposta("criar_canal_resposta", requisicao, "erro", "Nome de canal invalido")
        elif criar_canal(conexao, canal, usuario, timestamp):
            retorno = resposta("criar_canal_resposta", requisicao, mensagem="Canal criado")
        else:
            retorno = resposta("criar_canal_resposta", requisicao, "erro", "Canal ja existe")
        retorno["canal"] = canal
        if imprimir:
            print(f"[{rotulo}] CRIAR CANAL | nome: {canal} | resultado: {retorno['mensagem']}", flush=True)
        return retorno
    if tipo == "publicar":
        mensagem = requisicao.get("mensagem", "")
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}", canal) or not str(mensagem).strip():
            retorno = resposta("publicar_resposta", requisicao, "erro", "Canal ou mensagem invalidos")
            retorno["canal"] = canal
            return retorno
        criar_canal(conexao, canal, usuario, timestamp)
        publicacao = {
            "tipo": "publicacao", "usuario": usuario, "canal": canal,
            "mensagem": mensagem, "timestamp": agora(),
            "request_id": request_id, "servidor": identificador,
            "relogio_logico": RELOGIO.enviar(),
        }
        corpo = msgpack.packb(publicacao, use_bin_type=True)
        conexao.execute(
            "INSERT OR IGNORE INTO publicacoes "
            "(request_id, usuario, canal, mensagem, timestamp_envio, timestamp_servidor, conteudo) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (request_id, usuario, canal, mensagem, timestamp, publicacao["timestamp"], corpo),
        )
        registrar_evento(conexao, "enviada_pub", "publicacao", usuario, canal,
                         publicacao["timestamp"], corpo)
        pub.send_multipart([canal.encode("utf-8"), corpo])
        if registrar_mensagem is not None:
            registrar_mensagem("PUB")
        if imprimir:
            print(f"[{rotulo}] PUB ENVIADA -> {campos_legiveis(publicacao)}", flush=True)
            print(f"[{rotulo}] PUBLICAR | canal: {canal} | resultado: sucesso", flush=True)
        retorno = resposta("publicar_resposta", requisicao, mensagem="Publicacao enviada ao canal")
        retorno["canal"] = canal
        return retorno
    return resposta("erro", requisicao, "erro", "Operacao desconhecida")
def solicitar_referencia(contexto, endereco, tipo, identificador):
    req = contexto.socket(zmq.REQ)
    req.linger = 0
    req.rcvtimeo = 3500
    req.sndtimeo = 3500
    req.connect(endereco)
    pacote = {
        "tipo": tipo, "nome": identificador, "timestamp": agora(),
        "request_id": str(uuid.uuid4()), "relogio_logico": RELOGIO.enviar(),
    }
    try:
        print(f"[{rotulo_servidor(identificador)}] REF ENVIADO -> {campos_legiveis(pacote)}", flush=True)
        req.send(msgpack.packb(pacote, use_bin_type=True))
        retorno = msgpack.unpackb(req.recv(), raw=False)
        RELOGIO.receber(retorno)
        print(f"[{rotulo_servidor(identificador)}] REF RECEBIDO <- {campos_legiveis(retorno)}", flush=True)
        return retorno
    finally:
        req.close()
class Coordenacao:
    def __init__(self, contexto, identificador, rank, pub_endpoint, sub_endpoint,
                 porta_rpc, peers=None):
        self.contexto = contexto
        self.nome = identificador
        self.rank = int(rank)
        self.pub_endpoint = pub_endpoint
        self.sub_endpoint = sub_endpoint
        self.porta_rpc = int(porta_rpc)
        self.peers = peers or {}
        self.servidores = {identificador: self.rank}
        self.coordenador = None
        self.falhos = set()
        self.lock = threading.RLock()
        self.anunciado = threading.Event()
        self.ultima_eleicao = 0.0
        self.trava_eleicao = threading.Lock()  
        self.rodando = True
    def log(self, mensagem):
        print(f"[{rotulo_servidor(self.nome)}] {mensagem}", flush=True)
    def endereco(self, nome):
        return self.peers.get(nome, f"tcp://{nome}:5560")
    def mensagem(self, tipo, **campos):
        return {"tipo": tipo, "nome": self.nome, "timestamp": agora(),
                "request_id": str(uuid.uuid4()),
                "relogio_logico": RELOGIO.enviar(), **campos}
    def atualizar_servidores(self, lista):
        with self.lock:
            for item in lista:
                if "nome" in item and "rank" in item:
                    self.servidores[item["nome"]] = int(item["rank"])
            self.servidores[self.nome] = self.rank
            superiores = [n for n, r in self.servidores.items()
                          if r > self.rank and n not in self.falhos]
            dever_eleger = (self.coordenador is None or
                            (self.coordenador == self.nome and bool(superiores)))
        if dever_eleger:
            self.iniciar_eleicao("lista de servidores atualizada")
    def solicitar(self, destino, tipo, timeout=1100, **extra):
        requisicao = self.mensagem(tipo, **extra)
        socket = self.contexto.socket(zmq.REQ)
        socket.linger = 0
        socket.rcvtimeo = timeout
        socket.sndtimeo = timeout
        try:
            socket.connect(self.endereco(destino))
            socket.send(msgpack.packb(requisicao, use_bin_type=True))
            retorno = msgpack.unpackb(socket.recv(), raw=False)
            RELOGIO.receber(retorno)
            if retorno.get("request_id") != requisicao["request_id"]:
                raise ValueError("ID da resposta diferente do pedido")
            return retorno
        finally:
            socket.close()
    def definir_coordenador(self, nome, rank, origem):
        with self.lock:
            anterior = self.coordenador
            rank_atual = self.servidores.get(anterior, 0)
            if (origem == "PUB/SUB servers" and anterior and anterior not in self.falhos
                    and rank < rank_atual):
                return
            self.servidores[nome] = int(rank)
            self.falhos.discard(nome)
            self.coordenador = nome
            self.anunciado.set()
        if anterior != nome:
            self.log(f"COORDENADOR | {nome} | rank: {rank} | origem: {origem}")
    def anunciar(self):
        
        self.definir_coordenador(self.nome, self.rank, "eleicao Bully")
        publicador = self.contexto.socket(zmq.PUB)
        publicador.linger = 0
        try:
            publicador.connect(self.pub_endpoint)
            time.sleep(0.35)
            for _ in range(3):
                anuncio = self.mensagem("coordenador", coordenador=self.nome, rank=self.rank)
                publicador.send_multipart([b"servers", msgpack.packb(anuncio, use_bin_type=True)])
                time.sleep(0.12)
            self.log(f"ELEICAO | PUB topico servers | coordenador: {self.nome} | rank: {self.rank}")
        finally:
            publicador.close()
    def iniciar_eleicao(self, motivo="coordenador nao respondeu"):
        if not self.trava_eleicao.acquire(blocking=False):
            self.log("ELEICAO | ja existe uma eleicao local em andamento")
            return
        try:
            self._executar_eleicao(motivo)
        finally:
            self.trava_eleicao.release()

    def _executar_eleicao(self, motivo):
        with self.lock:
            if time.monotonic() - self.ultima_eleicao < 0.85:
                return
            self.ultima_eleicao = time.monotonic()
            self.anunciado.clear()
            superiores = sorted(((r, n) for n, r in self.servidores.items()
                                 if r > self.rank), reverse=True)
        self.log(f"ELEICAO INICIADA | motivo: {motivo} | superiores: {[n for _, n in superiores]}")
        respondeu = []
        for rank, nome in superiores:
            for tentativa in range(1, 3):
                try:
                    retorno = self.solicitar(nome, "eleicao", timeout=1400, rank=self.rank)
                    if retorno.get("status") != "OK":
                        raise ValueError(f"resposta inesperada: {retorno.get('status')}")
                    if int(retorno.get("rank", -1)) != rank:
                        raise ValueError("rank informado pelo peer nao corresponde ao registro")
                    respondeu.append(nome)
                    with self.lock:
                        self.falhos.discard(nome)
                    self.log(f"ELEICAO | {nome} respondeu OK | tentativa: {tentativa}")
                    break
                except (zmq.ZMQError, ValueError) as erro:
                    self.log(f"ELEICAO | falha consultando {nome} "
                             f"({self.endereco(nome)}) | tentativa: {tentativa}/2 "
                             f"| erro: {type(erro).__name__}: {erro}")
                    if tentativa < 2:
                        time.sleep(0.15)
            else:
                with self.lock:
                    self.falhos.add(nome)
                self.log(f"ELEICAO | {nome} nao respondeu apos 2 tentativas")
        if respondeu:
            self.anunciado.wait(timeout=4.0)
            maior_respondeu = max(self.servidores[n] for n in respondeu)
            with self.lock:
                lider = self.coordenador
                lider_rank = self.servidores.get(lider, 0)
                lider_falhou = lider in self.falhos
            if lider and not lider_falhou and lider_rank >= maior_respondeu:
                try:
                    confirmacao = self.solicitar(lider, "consultar_coordenador", timeout=1400)
                    if (confirmacao.get("status") == "OK" and
                            confirmacao.get("coordenador") == lider):
                        self.log(f"ELEICAO | anuncio confirmado | coordenador: {lider}")
                        return
                except (zmq.ZMQError, ValueError) as erro:
                    self.log(f"ELEICAO | anuncio de {lider} nao confirmado: {erro}")
                with self.lock:
                    self.falhos.add(lider)
                    if self.coordenador == lider:
                        self.coordenador = None
            for nome in respondeu:
                try:
                    estado = self.solicitar(nome, "consultar_coordenador", timeout=1400)
                    candidato = estado.get("coordenador", "")
                    rank_candidato = int(estado.get("rank_coordenador", 0))
                    with self.lock:
                        candidato_falhou = candidato in self.falhos
                    if (estado.get("status") == "OK" and candidato
                            and not candidato_falhou
                            and rank_candidato >= maior_respondeu):
                        self.definir_coordenador(candidato, rank_candidato,
                                                 "confirmacao REQ/REP")
                        return
                except (zmq.ZMQError, ValueError) as erro:
                    self.log(f"ELEICAO | consulta coordenador de {nome} falhou: {erro}")
            self.log("ELEICAO | servidor superior ativo, aguardando coordenador; "
                     "nao assumir lideranca")
            return
        self.anunciar()
    def _receber_rpc(self):
        rep = self.contexto.socket(zmq.REP)
        rep.linger = 0
        rep.bind(f"tcp://*:{self.porta_rpc}")
        self.log(f"REQ/REP SERVIDORES | ouvindo porta {self.porta_rpc}")
        try:
            while self.rodando:
                if not rep.poll(250, zmq.POLLIN):
                    continue
                try:
                    pedido = msgpack.unpackb(rep.recv(), raw=False)
                    if not isinstance(pedido, dict):
                        raise ValueError("pacote RPC precisa ser um dicionario")
                except (ValueError, TypeError, msgpack.exceptions.UnpackException) as erro:
                    self.log(f"RPC SERVIDORES | pacote invalido: {erro}")
                    rep.send(msgpack.packb({"tipo": "erro", "status": "erro",
                                            "mensagem": "pacote invalido"}, use_bin_type=True))
                    continue
                RELOGIO.receber(pedido)
                tipo = pedido.get("tipo", "")
                resposta = {"tipo": tipo + "_resposta", "request_id": pedido.get("request_id", ""),
                            "nome": self.nome, "timestamp": agora(), "status": "OK"}
                convocar_eleicao = False
                if tipo == "eleicao":
                    resposta["rank"] = self.rank
                    origem = pedido.get("nome", "")
                    rank_origem = int(pedido.get("rank", self.servidores.get(origem, -1)))
                    convocar_eleicao = self.rank > rank_origem
                    self.log(f"ELEICAO RPC RECEBIDA | origem: {origem} | rank: {rank_origem}")
                elif tipo == "consultar_coordenador":
                    with self.lock:
                        lider_atual = self.coordenador
                        resposta["coordenador"] = lider_atual or ""
                        resposta["rank_coordenador"] = self.servidores.get(lider_atual, 0)
                elif tipo == "hora_local":
                    resposta["hora_epoch"] = hora_aplicacao()
                elif tipo == "relogio":
                    with self.lock:
                        lider = self.coordenador
                    if lider == self.nome:
                        resposta["hora_epoch"] = hora_aplicacao()
                        resposta["coordenador"] = self.nome
                    else:
                        resposta["status"] = "erro"
                        resposta["coordenador"] = lider or ""
                else:
                    resposta["status"] = "erro"
                resposta["relogio_logico"] = RELOGIO.enviar()
                rep.send(msgpack.packb(resposta, use_bin_type=True))
                if tipo == "eleicao":
                    self.log(f"ELEICAO RPC RESPONDIDA | destino: {pedido.get('nome', '')} "
                             f"| rank: {self.rank}")
                    if convocar_eleicao:
                        threading.Thread(target=self.iniciar_eleicao,
                                         args=(f"pedido de {pedido.get('nome', '')}",),
                                         daemon=True).start()
        finally:
            rep.close()
    def _receber_anuncios(self):
        sub = self.contexto.socket(zmq.SUB)
        sub.linger = 0
        sub.connect(self.sub_endpoint)
        sub.setsockopt(zmq.SUBSCRIBE, b"servers")
        try:
            while self.rodando:
                if not sub.poll(250, zmq.POLLIN):
                    continue
                topico, corpo = sub.recv_multipart()
                if topico != b"servers":
                    continue
                anuncio = msgpack.unpackb(corpo, raw=False)
                RELOGIO.receber(anuncio)
                if anuncio.get("tipo") == "coordenador":
                    nome = anuncio.get("coordenador", "")
                    rank = int(anuncio.get("rank", 0))
                    if nome and rank:
                        with self.lock:
                            falhou_antes = nome in self.falhos
                        if falhou_antes:
                            try:
                                teste = self.solicitar(nome, "consultar_coordenador", timeout=900)
                                if (teste.get("status") != "OK" or
                                        teste.get("coordenador") != nome):
                                    self.log(f"ELEICAO | anuncio de {nome} ignorado (sem confirmacao)")
                                    continue
                            except (zmq.ZMQError, ValueError):
                                self.log(f"ELEICAO | anuncio de {nome} ignorado (sem resposta RPC)")
                                continue
                        self.definir_coordenador(nome, rank, "PUB/SUB servers")
        finally:
            sub.close()
    def iniciar(self):
        threading.Thread(target=self._receber_rpc, daemon=True).start()
        threading.Thread(target=self._receber_anuncios, daemon=True).start()
        time.sleep(0.18)
    def calcular_berkeley(self):
        with self.lock:
            pares = [n for n in self.servidores if n != self.nome and n not in self.falhos]
        desvios = [hora_aplicacao() - time.time()]
        participantes = [self.nome]
        for nome in pares:
            try:
                inicio = time.time()
                info = self.solicitar(nome, "hora_local", timeout=900)
                fim = time.time()
                if info.get("status") == "OK":
                    diferenca = float(info["hora_epoch"]) + (fim - inicio) / 2.0 - fim
                    desvios.append(diferenca)
                    participantes.append(nome)
            except (zmq.ZMQError, ValueError, KeyError):
                with self.lock:
                    self.falhos.add(nome)
        ajuste = sum(desvios) / len(desvios)
        definir_desvio(ajuste)
        self.log(f"BERKELEY | coordenador: {self.nome} | participantes: {participantes} | ajuste: {ajuste:+.4f}s")
        return hora_aplicacao()
    def sincronizar(self):
        with self.lock:
            lider = self.coordenador
        if not lider:
            self.iniciar_eleicao("sem coordenador")
            with self.lock:
                lider = self.coordenador
        if lider == self.nome:
            self.calcular_berkeley()
            return
        try:
            inicio = time.time()
            resposta = self.solicitar(lider, "relogio", timeout=2500)
            fim = time.time()
            if resposta.get("status") != "OK":
                raise ValueError("Coordenador recusou sincronizacao")
            ajuste = float(resposta["hora_epoch"]) + (fim - inicio) / 2.0 - fim
            definir_desvio(ajuste)
            self.log(f"RELOGIO FISICO | Berkeley | coordenador: {lider} | ajuste: {ajuste:+.4f}s")
        except (zmq.ZMQError, ValueError, KeyError):
            with self.lock:
                self.falhos.add(lider)
                self.coordenador = None
            self.log(f"COORDENADOR INDISPONIVEL | {lider}; iniciando eleicao")
            self.iniciar_eleicao(f"{lider} nao respondeu ao relogio")
            with self.lock:
                novo = self.coordenador
            if novo == self.nome:
                self.calcular_berkeley()
            elif novo:
                try:
                    self.sincronizar()
                except RecursionError:
                    self.log("Nao foi possivel sincronizar apos a eleicao")
class AgendadorSincronizacao:
    def __init__(self, coordenacao, rotulo, intervalo=15):
        self.coordenacao = coordenacao
        self.rotulo = rotulo
        self.intervalo = intervalo
        self.total = 0
        self.fila = queue.Queue()
        self.thread = threading.Thread(target=self._trabalhar, daemon=True)
        self.thread.start()
    def registrar(self, direcao):
        self.total += 1
        if self.total % self.intervalo == 0:
            marco = self.total
            print(f"[{self.rotulo}] SINCRONIZACAO | {marco} mensagens trocadas "
                  f"| evento: {direcao}", flush=True)
            self.fila.put(marco)
    def _trabalhar(self):
        while True:
            marco = self.fila.get()
            try:
                if marco is None:
                    return
                try:
                    self.coordenacao.sincronizar()
                except Exception as erro:
                    print(f"[{self.rotulo}] SINCRONIZACAO | marco: {marco} "
                          f"| erro: {erro}", flush=True)
            finally:
                self.fila.task_done()
    def parar(self):
        self.fila.put(None)
        self.thread.join(timeout=0.5)
def executar():
    identificador = os.getenv("SERVER_ID", "servidor1")
    endereco_broker = os.getenv("BROKER_ENDPOINT", "tcp://broker:5556")
    endereco_pub = os.getenv("PUB_ENDPOINT", "tcp://proxy:5557")
    banco = os.getenv("DB_PATH", "/app/data/bbs.db")
    endereco_ref = os.getenv("REF_ENDPOINT", "tcp://referencia:5559")
    endereco_sub = os.getenv("SUB_ENDPOINT", "tcp://proxy:5558")
    porta_rpc = int(os.getenv("SYNC_PORT", "5560"))
    peers = json.loads(os.getenv("PEER_ENDPOINTS", "{}"))
    rotulo = rotulo_servidor(identificador)
    Path(banco).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(banco, isolation_level=None) as conexao:
        criar_tabelas(conexao)
        contexto = zmq.Context()
        rep = contexto.socket(zmq.REP)
        pub = contexto.socket(zmq.PUB)
        rep.linger = 0
        pub.linger = 0
        pub.connect(endereco_pub)
        while True:
            try:
                rank = solicitar_referencia(contexto, endereco_ref, "rank", identificador)
                if rank.get("status") == "sucesso":
                    print(f"[{rotulo}] RANK REGISTRADO | {rank['rank']}", flush=True)
                    break
            except zmq.ZMQError as erro:
                print(f"[{rotulo}] Referencia indisponivel: {erro}", flush=True)
            time.sleep(2)
        coordenacao = Coordenacao(contexto, identificador, rank["rank"],
                                  endereco_pub, endereco_sub, porta_rpc, peers)
        coordenacao.iniciar()
        lista = solicitar_referencia(contexto, endereco_ref, "list", identificador)
        solicitar_referencia(contexto, endereco_ref, "heartbeat", identificador)
        coordenacao.atualizar_servidores(lista.get("servidores", []))
        print(f"[{rotulo}] SERVIDORES DISPONIVEIS | {lista.get('servidores', [])}", flush=True)
        mensagens_clientes = 0
        sincronizacao = AgendadorSincronizacao(coordenacao, rotulo)
        rep.connect(endereco_broker)
        print(f"[{rotulo}] PRONTO | REQ/REP: {endereco_broker} | "
              f"PUB/SUB: {endereco_pub} | SQLite: {banco}", flush=True)
        try:
            while True:
                recebido = rep.recv()
                sincronizacao.registrar("REQ")
                requisicao = msgpack.unpackb(recebido, raw=False)
                RELOGIO.receber(requisicao)
                print(f"[{rotulo}] RECEBIDO <- {campos_legiveis(requisicao)}", flush=True)
                registrar_evento(conexao, "recebida_req", requisicao.get("tipo", ""),
                                 requisicao.get("usuario", ""), requisicao.get("canal", ""),
                                 requisicao.get("timestamp", ""), recebido)
                resultado = processar(requisicao, conexao, pub, identificador,
                                     registrar_mensagem=sincronizacao.registrar)
                pacote = msgpack.packb(resultado, use_bin_type=True)
                registrar_evento(conexao, "enviada_rep", resultado["tipo"],
                                 resultado.get("usuario", ""), resultado.get("canal", ""),
                                 resultado["timestamp"], pacote)
                rep.send(pacote)
                sincronizacao.registrar("REP")
                print(f"[{rotulo}] ENVIADO -> {campos_legiveis(resultado)}", flush=True)
                mensagens_clientes += 1
                if mensagens_clientes % 10 == 0:
                    print(f"[{rotulo}] HEARTBEAT | apos {mensagens_clientes} mensagens de clientes", flush=True)
                    try:
                        solicitar_referencia(contexto, endereco_ref, "heartbeat", identificador)
                        lista = solicitar_referencia(contexto, endereco_ref, "list", identificador)
                        coordenacao.atualizar_servidores(lista.get("servidores", []))
                        print(f"[{rotulo}] SERVIDORES DISPONIVEIS | "
                              f"{lista.get('servidores', [])}", flush=True)
                    except zmq.ZMQError as erro:
                        print(f"[{rotulo}] Referencia indisponivel: {erro}", flush=True)
        finally:
            sincronizacao.parar()
            coordenacao.rodando = False
            rep.close()
            pub.close()
            contexto.term()

if __name__ == "__main__":
    executar()
