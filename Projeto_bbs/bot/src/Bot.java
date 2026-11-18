import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Bot {
    private static final int PUBLICACOES_POR_CICLO = 10;
    private static final int INTERVALO_MS = 1000;
    private static final Random sorteador = new Random();
    private static long relogioLogico = 0;
    private static synchronized long antesDeEnviar() {
        return ++relogioLogico;
    }
    private static synchronized void aoReceber(Map<String, Object> dados) {
        Object valor = dados.get("relogio_logico");
        if (valor != null) {
            relogioLogico = Math.max(relogioLogico, Long.parseLong(valor.toString()));
        }
    }
    private static String agora() {
        return Instant.now().toString();
    }
    private static String rotulo(String nome) {
        if (nome.matches("bot[0-9]+")) {
            return "Bot " + nome.substring(3);
        }
        return nome;
    }
    private static String nomeCampo(String chave) {
        return switch (chave) {
            case "usuario" -> "nome";
            case "timestamp" -> "horario";
            case "request_id" -> "id";
            case "status" -> "resultado";
            default -> chave;
        };
    }
    private static String mensagemLegivel(Map<String, Object> mapa) {
        List<String> campos = new ArrayList<>();
        for (Map.Entry<String, Object> e : mapa.entrySet()) {
            campos.add(nomeCampo(e.getKey()) + ": " + e.getValue());
        }
        return String.join(" | ", campos);
    }
    private static byte[] empacotar(Map<String, Object> dados) throws IOException {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(dados.size());
            for (Map.Entry<String, Object> entrada : dados.entrySet()) {
                packer.packString(entrada.getKey());
                if (entrada.getValue() instanceof Number numero) {
                    packer.packLong(numero.longValue());
                } else {
                    packer.packString(entrada.getValue().toString());
                }
            }
            return packer.toByteArray();
        }
    }
    private static Map<String, Object> desempacotar(byte[] bytes) throws IOException {
        Value valor;
        try (var unpacker = MessagePack.newDefaultUnpacker(bytes)) {
            valor = unpacker.unpackValue();
        }
        Map<String, Object> mapa = new LinkedHashMap<>();
        MapValue valores = valor.asMapValue();
        for (Map.Entry<Value, Value> entrada : valores.map().entrySet()) {
            String chave = entrada.getKey().asStringValue().asString();
            Value conteudo = entrada.getValue();
            if (conteudo.isArrayValue()) {
                List<String> itens = new ArrayList<>();
                for (Value item : conteudo.asArrayValue().list()) {
                    itens.add(item.asStringValue().asString());
                }
                mapa.put(chave, itens);
            } else if (conteudo.isIntegerValue()) {
                mapa.put(chave, conteudo.asIntegerValue().toLong());
            } else if (conteudo.isStringValue()) {
                mapa.put(chave, conteudo.asStringValue().asString());
            } else {
                mapa.put(chave, conteudo.toString());
            }
        }
        return mapa;
    }
    private static Map<String, Object> solicitar(ZContext contexto, String destino, String usuario,
                                                   String tipo, String canal, String texto)
            throws IOException {
        Map<String, Object> requisicao = new LinkedHashMap<>();
        requisicao.put("tipo", tipo);
        requisicao.put("usuario", usuario);
        requisicao.put("timestamp", agora());
        requisicao.put("request_id", UUID.randomUUID().toString());
        requisicao.put("relogio_logico", antesDeEnviar());
        if (canal != null) requisicao.put("canal", canal);
        if (texto != null) requisicao.put("mensagem", texto);
        ZMQ.Socket req = contexto.createSocket(ZMQ.REQ);
        req.setLinger(0);
        req.setReceiveTimeOut(5000);
        req.setSendTimeOut(5000);
        req.connect(destino);
        try {
            System.out.printf("[%s] ENVIADO -> %s%n", rotulo(usuario), mensagemLegivel(requisicao));
            if (!req.send(empacotar(requisicao), 0)) {
                throw new IOException("Falha ao enviar requisicao");
            }
            byte[] resposta = req.recv(0);
            if (resposta == null) throw new IOException("Servidor indisponivel (timeout)");
            Map<String, Object> dados = desempacotar(resposta);
            aoReceber(dados);
            System.out.printf("[%s] RECEBIDO <- %s%n", rotulo(usuario), mensagemLegivel(dados));
            return dados;
        } finally {
            req.close();
        }
    }
    private static Map<String, Object> solicitar(ZContext ctx, String destino, String nome, String tipo)
            throws IOException {
        return solicitar(ctx, destino, nome, tipo, null, null);
    }
    private static List<String> listarCanais(ZContext contexto, String destino, String nome)
            throws IOException {
        System.out.printf("[%s] LISTANDO CANAIS%n", rotulo(nome));
        Map<String, Object> resposta = solicitar(contexto, destino, nome, "listar_canais");
        List<String> canais = new ArrayList<>();
        Object dados = resposta.get("canais");
        if (dados instanceof List<?> lista) {
            for (Object item : lista) canais.add(item.toString());
        }
        System.out.printf("[%s] CANAIS DISPONIVEIS | %s%n", rotulo(nome),
                canais.isEmpty() ? "(nenhum)" : String.join(", ", canais));
        return canais;
    }
    // Parte 2 ------------------------------------------------------------
    private static List<String> criarCanalSeNecessario(ZContext contexto, String destino,
                                                        String nome, List<String> canais)
            throws IOException {
        if (canais.size() >= 5) return canais;
        String novo;
        if (!canais.contains("geral")) {
            novo = "geral";
        } else {
            int indice = 1;
            while (canais.contains("canal_" + nome + "_" + indice)) indice++;
            novo = "canal_" + nome + "_" + indice;
        }
        System.out.printf("[%s] CRIANDO CANAL | nome: %s%n", rotulo(nome), novo);
        Map<String, Object> resposta = solicitar(contexto, destino, nome, "criar_canal", novo, null);
        if ("sucesso".equals(resposta.get("status"))) {
            canais.add(novo);
            Collections.sort(canais);
            System.out.printf("[%s] CANAL CRIADO | nome: %s%n", rotulo(nome), novo);
        } else {
            System.out.printf("[%s] CANAL NAO CRIADO | %s%n", rotulo(nome), resposta.get("mensagem"));
        }
        return canais;
    }
    private static void inscrever(ZMQ.Socket sub, String nome, List<String> canais, Set<String> inscricoes) {
        List<String> candidatos = new ArrayList<>(canais);
        candidatos.removeAll(inscricoes);
        if (inscricoes.size() < 3 && !candidatos.isEmpty()) {
            String canal = candidatos.remove(sorteador.nextInt(candidatos.size()));
            sub.subscribe(canal.getBytes(StandardCharsets.UTF_8));
            inscricoes.add(canal);
            System.out.printf("[%s] INSCRITO NO CANAL | %s%n", rotulo(nome), canal);
        }
    }
    private static void receberPublicacoes(ZMQ.Poller poller, int indice, ZMQ.Socket subscriber,
                                            String nome, Set<String> inscricoes, int esperaMs)
            throws IOException {
        long termino = System.nanoTime() + esperaMs * 1_000_000L;
        while (true) {
            long faltandoMs = (termino - System.nanoTime()) / 1_000_000L;
            if (faltandoMs <= 0) break;
            if (poller.poll(Math.max(1, faltandoMs)) <= 0 || !poller.pollin(indice)) continue;
            byte[] topico = subscriber.recv(0);
            byte[] corpo = subscriber.recv(0);
            if (topico == null || corpo == null) continue;
            String canal = new String(topico, StandardCharsets.UTF_8);
            Map<String, Object> publicacao = desempacotar(corpo);
            aoReceber(publicacao);
            if (!inscricoes.contains(canal)) continue;
            System.out.printf("[%s] PUB RECEBIDA <- canal: %s | %s | recebido_em: %s%n",
                    rotulo(nome), canal, mensagemLegivel(publicacao), agora());
        }
    }
    public static void main(String[] args) throws Exception {
        String nome = System.getenv().getOrDefault("BOT_NAME", "bot1");
        String destino = System.getenv().getOrDefault("BROKER_ENDPOINT", "tcp://broker:5555");
        String enderecoSub = System.getenv().getOrDefault("SUB_ENDPOINT", "tcp://proxy:5558");
        Set<String> inscricoes = new LinkedHashSet<>();
        String[] frases = {"Ola, pessoal!", "Noticia do canal", "Atualizacao do sistema",
                           "Teste de publicacao", "Mensagem distribuida"};
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket subscriber = contexto.createSocket(ZMQ.SUB);
            subscriber.setLinger(0);*
            subscriber.connect(enderecoSub);
            try (ZMQ.Poller poller = contexto.createPoller(1)) {
                int indice = poller.register(subscriber, ZMQ.Poller.POLLIN);
                System.out.printf("[%s] Iniciado | broker: %s | proxy: %s%n",
                        rotulo(nome), destino, enderecoSub);
                // Login
                while (true) {
                    try {
                        System.out.printf("[%s] TENTATIVA DE LOGIN | nome: %s%n", rotulo(nome), nome);
                        Map<String, Object> login = solicitar(contexto, destino, nome, "login");
                        if ("sucesso".equals(login.get("status"))) {
                            System.out.printf("[%s] LOGIN OK | nome: %s | servidor: %s%n",
                                    rotulo(nome), nome, login.get("servidor"));
                            break;
                        }
                    } catch (IOException falha) {
                        System.out.printf("[%s] Tentativa de login sem resposta: %s%n",
                                rotulo(nome), falha.getMessage());
                    }
                    Thread.sleep(2000);
                }
                while (true) {
                    try {
                        List<String> canais = listarCanais(contexto, destino, nome);
                        canais = criarCanalSeNecessario(contexto, destino, nome, canais);
                        inscrever(subscriber, nome, canais, inscricoes);
                        if (canais.isEmpty()) {
                            Thread.sleep(2000);
                            continue;
                        }
                        Thread.sleep(400);
                        String canal = canais.get(sorteador.nextInt(canais.size()));
                        System.out.printf("[%s] INICIANDO CICLO | canal: %s | 10 publicacoes | intervalo: 1 segundo%n",
                                rotulo(nome), canal);
                        for (int numero = 1; numero <= PUBLICACOES_POR_CICLO; numero++) {
                            String texto = frases[sorteador.nextInt(frases.length)]
                                    + " (" + nome + ", mensagem " + numero + ")";
                            System.out.printf("[%s] MENSAGEM %d/%d | publicando no canal: %s%n",
                                    rotulo(nome), numero, PUBLICACOES_POR_CICLO, canal);
                            Map<String, Object> resposta = solicitar(contexto, destino, nome,
                                    "publicar", canal, texto);
                            System.out.printf("[%s] PUBLICACAO | %d/%d | %s | canal: %s | servidor: %s%n",
                                    rotulo(nome), numero, PUBLICACOES_POR_CICLO, resposta.get("status"),
                                    canal, resposta.get("servidor"));
                            receberPublicacoes(poller, indice, subscriber, nome, inscricoes, INTERVALO_MS);
                        }
                        System.out.printf("[%s] CICLO CONCLUIDO | 10 publicacoes%n", rotulo(nome));
                    } catch (IOException falha) {
                        System.out.printf("[%s] Comunicacao indisponivel: %s%n",
                                rotulo(nome), falha.getMessage());
                        Thread.sleep(2000);
                    }
                }
            }
        }
    }
}
