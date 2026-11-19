# Projeto Sistemas Distribuidos 

Projeto integrado de Bulletin Board System (BBS) com dois bots Java e dois servidores Python. Nao tive tempo de incluir a replicacao (parte 5 do projeto)

## Tecnologias 

- Java 21 (JeroMQ e MessagePack): bots que fazem login, consultam canais, criam canais quando necessario e publicam mensagens automaticamente.
- Python 3.12 (pyzmq, msgpack, SQLite): quatro servidores que processam requisicoes, publicam mensagens via ZeroMQ e registram dados em bancos independentes.
- Broker: encaminha pedidos REQ/REP entre bots e servidores por balanceamento automatico do ZeroMQ.
- Proxy: distribui as publicacoes para os bots inscritos nos canais.
- Referencia: atribui ranks, informa servidores disponiveis e recebe heartbeats.
- Docker Compose: inicia os 9 containers em uma rede interna.


## Executar

Requisitos: Docker Desktop instalado e iniciado. Abra um terminal dentro da pasta e execute: docker compose up --build

Para acompanhar apenas os servidores e a referencia, use em outro terminal: docker compose logs -f servidor1 servidor2 servidor3 servidor4

Para acompanhar os bots: docker compose logs -f bot1 bot2

Para acompanhar as eleicoes a coordenador: docker compose logs -f servidor1 servidor2 servidor3 servidor4 | Select-String 'ELEICAO|COORDENADOR|BERKELEY'

Para encerrar, useP: Ctrl+C

## Funcionamento

### Login e canais

Os bots realizam login apenas pelo nome e consultam os canais disponiveis. Quando existem menos de cinco canais, podem criar outro. Cada servidor mantem seu proprio banco SQLite.

### Publicacoes

Cada bot se inscreve em ate tres canais e, em cada ciclo, envia dez publicacoes, com intervalo de um segundo. Os servidores publicam mensagens no proxy PUB/SUB e os bots recebem as mensagens dos canais inscritos. Os ciclos se repetem automaticamente.

### Relogios e heartbeat

Cada bot e servidor utiliza um relogio logico: incrementa antes de enviar e, ao receber, atualiza pelo maximo entre o valor local e o recebido.

O processo de referencia registra os ranks e os servidores ativos. Cada servidor envia um heartbeat inicial e outro a cada dez requisicoes de clientes processadas.

### Eleicao Bully e Berkeley

Os servidores se comunicam diretamente por REQ/REP pela porta interna, usando MessagePack. O algoritmo Bully elege o servidor disponivel de maior rank como coordenador, que anuncia a eleicao no topico servers.Na sincronizacao Berkeley, o coordenador consulta o horario virtual dos servidores acessiveis e calcula uma media dos desvios, considerando aproximadamente o atraso de rede. Os ajustes sao aplicados apenas ao relogio da aplicacao, sem modificar o relogio do container. Cada servidor contabiliza as mensagens REQ recebidas, REP enviadas e PUB enviadas e agenda sincronizacao nos marcos 15, 30, 45, ... .

