#!/usr/bin/env python3
"""Experimentos EventBridge Classic -> SQS Standard (AWS real, us-east-2).

Não cria/apaga recursos. Não chama PurgeQueue. Só exclui mensagens que
pertencem ao RUN_ID gerado por este programa após registrá-las em memória.
"""

# Ajuste 2026-11-14: leituras de fila repetidas e pausa >=60 s; suporte a
# --inicio-repeticao para completar cargas interrompidas sem repetir amostras.
import argparse
import csv
import json
import os
import statistics
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

REGIAO = "us-east-2"
CONTA_ESPERADA = os.getenv("AWS_EXPECTED_ACCOUNT_ID", "").strip()
BARRAMENTO = "pedidos"
FONTE = "sbc.pedidos"
TIPO_EVENTO = "PedidoCriado"
REGRAS = {"normal": "pedidos-normais", "prioritario": "pedidos-prioritario"}
FILAS = {"normal": "pedidos-normais", "prioritario": "pedidos-prioritario"}
MAX_TOTAL_EVENTOS_POR_EXECUCAO = 10000  # limite total de publicação por invocação
MAX_POR_CENARIO = 1000
MAX_REPETICOES = 5
CONFIG = Config(retries={"max_attempts": 4, "mode": "standard"})
CAMPOS_ENVIO = ["run_id", "cenario", "repeticao", "evento_id", "tipo", "fila_esperada", "enviado_utc", "aceito", "eventbridge_id", "erro_publicacao"]
CAMPOS_RECEBIMENTO = ["run_id", "cenario", "repeticao", "fila", "evento_id", "tipo", "enviado_utc", "recebido_utc", "latencia_ms", "sqs_message_id", "sqs_receive_count", "delete_sucesso"]
CAMPOS_RESUMO = ["run_id", "cenario", "repeticao", "eventos_planejados", "eventos_aceitos", "esperados_na_fila", "entregues_corretamente_unicos", "faltantes", "fora_da_rota", "duplicacoes", "latencia_media_ms", "latencia_p95_ms", "vazao_publicacao_eventos_s", "vazao_entrega_eventos_s", "duracao_publicacao_s", "tempo_recuperacao_s", "backlog_antes_reinicio_aprox", "tempo_observacao_s", "erros_consumidor", "erros_exclusao"]


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat(timespec="microseconds")


def salvar_csv(caminho, campos, linhas):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(linhas)


def anexar_csv(caminho, campos, linha):
    existe = caminho.exists() and caminho.stat().st_size > 0
    with caminho.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
        if not existe:
            writer.writeheader()
        writer.writerow(linha)


def percentil95(valores):
    if not valores:
        return ""
    a = sorted(valores)
    pos = 0.95 * (len(a) - 1)
    baixo = int(pos)
    alto = min(baixo + 1, len(a) - 1)
    return round(a[baixo] + (a[alto] - a[baixo]) * (pos - baixo), 3)


def attr_queues(sqs, urls):
    resultado = {}
    for tipo, url in urls.items():
        a = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"],
        )["Attributes"]
        resultado[tipo] = {
            "disponiveis": int(a.get("ApproximateNumberOfMessages", 0)),
            "invisiveis": int(a.get("ApproximateNumberOfMessagesNotVisible", 0)),
            "atrasadas": int(a.get("ApproximateNumberOfMessagesDelayed", 0)),
        }
    return resultado



def aguardar_filas_vazias(sqs, urls, limite_s=180, intervalo_s=10):
    """Somente consulta SQS; não recebe nem exclui mensagens.

    Exige duas leituras consecutivas zeradas para minimizar falsos bloqueios
    provocados por contadores aproximados eventualmente consistentes.
    """
    inicio = time.monotonic()
    zeros_consecutivos = 0
    ultimo_estado = {}
    while True:
        ultimo_estado = attr_queues(sqs, urls)
        ocupadas = {tipo: info for tipo, info in ultimo_estado.items()
                    if sum(info.values()) > 0}
        if not ocupadas:
            zeros_consecutivos += 1
            print(f"[SQS] Filas aparentemente vazias ({zeros_consecutivos}/2 verificações).", flush=True)
            if zeros_consecutivos >= 2:
                return
        else:
            zeros_consecutivos = 0
            print(f"[SQS] Aguardando contadores zerarem: {ocupadas}", flush=True)
        restante = limite_s - (time.monotonic() - inicio)
        if restante <= 0:
            raise RuntimeError(
                "As filas ainda indicam mensagens após a espera; "
                f"nenhuma mensagem foi apagada. Contagens aproximadas: {ultimo_estado}. "
                "Verifique os CSVs da última repetição antes de prosseguir."
            )
        time.sleep(min(intervalo_s, restante))

def preflight(sts, events, sqs, aguardar_fila_s):
    conta = sts.get_caller_identity()["Account"]
    if CONTA_ESPERADA and conta != CONTA_ESPERADA:
        raise RuntimeError(f"CONTA INCORRETA: conta atual {conta}; esperada {CONTA_ESPERADA}.")
    events.describe_event_bus(Name=BARRAMENTO)
    urls = {}
    for tipo, nome in FILAS.items():
        url = sqs.get_queue_url(QueueName=nome)["QueueUrl"]
        urls[tipo] = url
        regra = events.describe_rule(Name=REGRAS[tipo], EventBusName=BARRAMENTO)
        if regra.get("State") != "ENABLED":
            raise RuntimeError(f"Regra {REGRAS[tipo]} não está ENABLED: {regra.get('State')}")
        alvos = events.list_targets_by_rule(Rule=REGRAS[tipo], EventBusName=BARRAMENTO)["Targets"]
        arn = f"arn:aws:sqs:{REGIAO}:{conta}:{nome}"
        alvos_certos = [a for a in alvos if a["Arn"] == arn]
        if not alvos_certos:
            raise RuntimeError(f"Regra {REGRAS[tipo]} não aponta para {arn}.")
        if any(a.get("RoleArn") for a in alvos_certos):
            raise RuntimeError(f"Regra {REGRAS[tipo]} ainda tem RoleArn. Reveja a configuração antes do teste.")
        politica_str = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["Policy"])["Attributes"].get("Policy", "")
        if not politica_str:
            raise RuntimeError(f"A fila {nome} não tem policy de acesso.")
        p = json.loads(politica_str)
        arn_regra = f"arn:aws:events:{REGIAO}:{conta}:rule/{BARRAMENTO}/{REGRAS[tipo]}"
        def autoriza(statement):
            principal = statement.get("Principal", {})
            if isinstance(principal, str):
                return False
            servico = principal.get("Service", [])
            if isinstance(servico, str): servico = [servico]
            actions = statement.get("Action", [])
            if isinstance(actions, str): actions = [actions]
            condicao = statement.get("Condition", {}).get("ArnEquals", {})
            return (statement.get("Effect") == "Allow"
                    and "events.amazonaws.com" in servico
                    and "sqs:SendMessage" in actions
                    and statement.get("Resource") == arn
                    and condicao.get("aws:SourceArn") == arn_regra)
        if not any(autoriza(s) for s in p.get("Statement", [])):
            raise RuntimeError(f"Policy da fila {nome} não inclui Allow para {arn_regra}.")
    aguardar_filas_vazias(sqs, urls, limite_s=aguardar_fila_s)
    print(f"[OK] Conta {conta}, região {REGIAO}, barramento {BARRAMENTO}, duas regras, duas policies e destinos verificados.")
    print("[OK] Filas aparentemente vazias (contagem aproximada do SQS). Não execute outros consumidores simultaneamente.")
    return urls


def preparar_eventos(cenario, repeticao, run_id, qtd, invalidos):
    # IDs gerados uma vez; não reutilizamos teste-001 ou outros eventos antigos.
    eventos = []
    for i in range(qtd):
        tipo = "normal" if i % 2 == 0 else "prioritario"
        eventos.append({"evento_id": str(uuid.uuid4()), "tipo": tipo, "fila_esperada": FILAS[tipo]})
    for i in range(invalidos):
        eventos.append({"evento_id": str(uuid.uuid4()), "tipo": "desconhecido", "fila_esperada": ""})
    return eventos


def coletor(tipo_fila, url, run_id, cenario, repeticao, sqs, recebidos, mutex, stop_event, erros):
    # Cada thread usa o mesmo client boto3, operação thread-safe para chamadas usuais.
    while not stop_event.is_set():
        try:
            resp = sqs.receive_message(
                QueueUrl=url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
                MessageSystemAttributeNames=["ApproximateReceiveCount"],
            )
            for msg in resp.get("Messages", []):
                momento = utc_now()
                try:
                    body = json.loads(msg["Body"])
                    detail = body.get("detail", {})
                except (ValueError, TypeError, AttributeError):
                    detail = {}
                # Não apaga mensagens antigas ou de outros produtores.
                if detail.get("run_id") != run_id:
                    with mutex:
                        erros.append(f"Mensagem desconhecida na fila {tipo_fila}; deixada intacta: {msg.get('MessageId')}")
                    continue
                enviado = detail.get("instante_envio", "")
                latencia = ""
                try:
                    latencia = round((momento - datetime.fromisoformat(enviado)).total_seconds() * 1000, 3)
                except (ValueError, TypeError):
                    pass
                r = {
                    "run_id": run_id, "cenario": cenario, "repeticao": repeticao,
                    "fila": FILAS[tipo_fila], "evento_id": detail.get("evento_id", ""),
                    "tipo": detail.get("tipo", ""), "enviado_utc": enviado,
                    "recebido_utc": momento.isoformat(timespec="microseconds"),
                    "latencia_ms": latencia, "sqs_message_id": msg.get("MessageId", ""),
                    "sqs_receive_count": msg.get("Attributes", {}).get("ApproximateReceiveCount", ""),
                    "delete_sucesso": False,
                }
                # Guarda o registro antes de solicitar exclusão; eventuais exclusões falhas são registradas.
                try:
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
                    r["delete_sucesso"] = True
                except (BotoCoreError, ClientError) as exc:
                    with mutex:
                        erros.append(f"Falha ao excluir {msg.get('MessageId')}: {exc}")
                with mutex:
                    recebidos.append(r)
        except (BotoCoreError, ClientError) as exc:
            with mutex:
                erros.append(f"Erro no consumidor {tipo_fila}: {exc}")
            stop_event.set()
            break


def enviar(eventos, run_id, cenario, repeticao, events):
    envios = []
    inicio_monotonico = time.monotonic()
    for pos in range(0, len(eventos), 10):
        lote = eventos[pos:pos + 10]
        entries = []
        registros = []
        for e in lote:
            instante = iso_now()
            detail = {"run_id": run_id, "evento_id": e["evento_id"], "tipo": e["tipo"], "valor": 100, "instante_envio": instante}
            entries.append({"Source": FONTE, "DetailType": TIPO_EVENTO, "EventBusName": BARRAMENTO, "Detail": json.dumps(detail)})
            registros.append({"run_id": run_id, "cenario": cenario, "repeticao": repeticao,
                             "evento_id": e["evento_id"], "tipo": e["tipo"], "fila_esperada": e["fila_esperada"],
                             "enviado_utc": instante, "aceito": False, "eventbridge_id": "", "erro_publicacao": ""})
        resposta = events.put_events(Entries=entries)
        for registro, retorno in zip(registros, resposta["Entries"]):
            registro["aceito"] = bool(retorno.get("EventId"))
            registro["eventbridge_id"] = retorno.get("EventId", "")
            registro["erro_publicacao"] = retorno.get("ErrorCode", "") + ":" + retorno.get("ErrorMessage", "") if retorno.get("ErrorCode") else ""
            envios.append(registro)
        if resposta.get("FailedEntryCount"):
            print(f"[AVISO] {resposta['FailedEntryCount']} entrada(s) recusada(s) pela API neste lote.")
    duracao = max(time.monotonic() - inicio_monotonico, 1e-9)
    return envios, duracao


def extrair_metricas(run_id, cenario, repeticao, envios, recebidos, duracao_envio, tempo_recuperacao, backlog_inicio, observacao_s, erros):
    aceitos = {a["evento_id"]: a for a in envios if a["aceito"]}
    esperados = {k: a for k, a in aceitos.items() if a["fila_esperada"]}
    corretos = {}
    fora_da_rota = 0
    duplicacoes = 0
    latencias = []
    for r in sorted(recebidos, key=lambda x: x["recebido_utc"]):
        id_evt = r["evento_id"]
        a = aceitos.get(id_evt)
        if not a or not a["fila_esperada"] or a["fila_esperada"] != r["fila"]:
            fora_da_rota += 1
            continue
        if id_evt in corretos:
            duplicacoes += 1
        else:
            corretos[id_evt] = r
            if isinstance(r["latencia_ms"], (int, float)) and r["latencia_ms"] >= 0:
                latencias.append(r["latencia_ms"])
    faltantes = len(set(esperados) - set(corretos))
    vazao_entrega = ""
    if corretos:
        t_inicio = min(datetime.fromisoformat(a["enviado_utc"]) for a in envios if a["aceito"])
        t_ultimo = max(datetime.fromisoformat(r["recebido_utc"]) for r in corretos.values())
        janela = max((t_ultimo - t_inicio).total_seconds(), 0.001)
        vazao_entrega = round(len(corretos) / janela, 3)
    metricas = {
        "run_id": run_id, "cenario": cenario, "repeticao": repeticao,
        "eventos_planejados": len(envios), "eventos_aceitos": len(aceitos),
        "esperados_na_fila": len(esperados), "entregues_corretamente_unicos": len(corretos),
        "faltantes": faltantes, "fora_da_rota": fora_da_rota, "duplicacoes": duplicacoes,
        "latencia_media_ms": round(statistics.mean(latencias), 3) if latencias else "",
        "latencia_p95_ms": percentil95(latencias),
        "vazao_publicacao_eventos_s": round(len(aceitos) / duracao_envio, 3),
        "vazao_entrega_eventos_s": vazao_entrega,
        "duracao_publicacao_s": round(duracao_envio, 3),
        "tempo_recuperacao_s": tempo_recuperacao,
        "backlog_antes_reinicio_aprox": backlog_inicio,
        "tempo_observacao_s": round(observacao_s, 3),
        "erros_consumidor": sum(1 for e in erros if "Erro no consumidor" in e),
        "erros_exclusao": sum(1 for e in erros if "Falha ao excluir" in e),
    }
    return metricas


def executar_um(cenario, repeticao, qtd, invalidos, espera_sem_consumidor, limite_entrega_s, events, sqs, urls, saida):
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    print(f"\n=== {cenario.upper()} | repetição {repeticao} | {qtd} eventos de pedidos | run_id={run_id} ===", flush=True)
    # A checagem de filas vazias é feita no inicio e entre as repetições.
    # Não se deve tomar a decisão com uma única leitura de contador aproximado.
    planejados = preparar_eventos(cenario, repeticao, run_id, qtd, invalidos)
    stop_event = threading.Event()
    mutex = threading.Lock()
    recebidos, erros = [], []
    threads = []
    amostras = []
    inicio_total = time.monotonic()

    def iniciar_consumidores():
        for tipo, url in urls.items():
            th = threading.Thread(target=coletor, args=(tipo, url, run_id, cenario, repeticao, sqs, recebidos, mutex, stop_event, erros), daemon=True)
            th.start()
            threads.append(th)
        time.sleep(0.3)

    def amostrar(phase):
        for tipo, info in attr_queues(sqs, urls).items():
            amostras.append({"run_id": run_id, "cenario": cenario, "repeticao": repeticao,
                             "fase": phase, "fila": FILAS[tipo], "segundos_desde_inicio": round(time.monotonic() - inicio_total, 3),
                             "disponiveis_aprox": info["disponiveis"], "invisiveis_aprox": info["invisiveis"], "atrasadas_aprox": info["atrasadas"]})

    if cenario != "outage":
        iniciar_consumidores()
    envios, tempo_publicacao = enviar(planejados, run_id, cenario, repeticao, events)
    salvar_csv(saida / f"envios_{run_id}.csv", CAMPOS_ENVIO, envios)
    qtd_esperada = sum(1 for a in envios if a["aceito"] and a["fila_esperada"])
    print(f"Publicação concluída: {sum(a['aceito'] for a in envios)} aceitos; {qtd_esperada} esperados nas filas.", flush=True)
    backlog_inicio = ""
    tempo_recuperacao = ""
    inicio_recuperacao = None
    if cenario == "outage":
        print(f"Consumidores DESLIGADOS por {espera_sem_consumidor} s. Registrando backlog (contadores aproximados).", flush=True)
        fim = time.monotonic() + espera_sem_consumidor
        while time.monotonic() < fim:
            amostrar("consumidor_desligado")
            time.sleep(min(2, max(fim - time.monotonic(), 0)))
        amostrar("antes_do_reinicio")
        ultimo_snapshot = attr_queues(sqs, urls)
        backlog_inicio = sum(d["disponiveis"] + d["invisiveis"] for d in ultimo_snapshot.values())
        inicio_recuperacao = time.monotonic()
        iniciar_consumidores()
    else:
        # Roteamento/performance: recebem continuamente desde antes do primeiro envio.
        pass

    ultimo_check = time.monotonic()
    fim_observacao = time.monotonic() + limite_entrega_s
    completado_em = None
    try:
        while time.monotonic() < fim_observacao:
            with mutex:
                ids = {r["evento_id"] for r in recebidos if r["evento_id"] in {a["evento_id"] for a in envios if a["aceito"] and a["fila_esperada"]}}
                erros_consumidor = any("Erro no consumidor" in e for e in erros)
            if erros_consumidor:
                break
            if len(ids) >= qtd_esperada and completado_em is None:
                completado_em = time.monotonic()
                if cenario == "outage" and inicio_recuperacao is not None:
                    tempo_recuperacao = round(completado_em - inicio_recuperacao, 3)
            if completado_em is not None and time.monotonic() - completado_em >= 3:
                break  # janela de 3s para observar eventuais duplicações
            if cenario == "outage" and time.monotonic() - ultimo_check >= 2:
                amostrar("consumidor_reiniciado")
                ultimo_check = time.monotonic()
            time.sleep(0.2)
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=4)
    if cenario == "outage":
        amostrar("fim")
        salvar_csv(saida / f"backlog_{run_id}.csv",
                   ["run_id", "cenario", "repeticao", "fase", "fila", "segundos_desde_inicio", "disponiveis_aprox", "invisiveis_aprox", "atrasadas_aprox"], amostras)
    salvar_csv(saida / f"recebimentos_{run_id}.csv", CAMPOS_RECEBIMENTO, recebidos)
    salvar_csv(saida / f"erros_{run_id}.csv", ["erro"], [{"erro": e} for e in erros])
    total = time.monotonic() - inicio_total
    m = extrair_metricas(run_id, cenario, repeticao, envios, recebidos, tempo_publicacao, tempo_recuperacao, backlog_inicio, total, erros)
    anexar_csv(saida / "resumo.csv", CAMPOS_RESUMO, m)
    print("RESULTADO:", json.dumps({k: m[k] for k in ["eventos_planejados", "eventos_aceitos", "esperados_na_fila", "entregues_corretamente_unicos", "faltantes", "fora_da_rota", "duplicacoes", "latencia_media_ms", "latencia_p95_ms", "vazao_entrega_eventos_s", "tempo_recuperacao_s"]}, ensure_ascii=False), flush=True)
    if erros:
        print(f"[ATENÇÃO] {len(erros)} ocorrência(s) em erros_{run_id}.csv")
    if m["faltantes"] or m["fora_da_rota"] or m["erros_consumidor"] or m["erros_exclusao"]:
        print("[ATENÇÃO] Resultado incompleto/inesperado. Não execute o lote seguinte sem investigar.")
    return m


def main():
    ap = argparse.ArgumentParser(description="Testes REAIS e limitados EventBridge -> SQS, sem criar recursos.")
    ap.add_argument("--cenario", choices=["smoke", "roteamento", "carga", "indisponibilidade"], required=True)
    ap.add_argument("--qtd", type=int, default=None, help="Quantidade de pedidos no teste (máx. 1000)")
    ap.add_argument("--repeticoes", type=int, default=1, help="Quantidade de repetições a executar (máx. 5)")
    ap.add_argument("--inicio-repeticao", type=int, default=1, help="Rótulo da primeira repetição (usar 3 ao retomar após 1 e 2)")
    ap.add_argument("--pausa-repeticoes", type=int, default=70, help="Segundos de estabilização entre repetições, padrão 70")
    ap.add_argument("--aguardar-fila", type=int, default=180, help="Tempo máximo para verificar filas vazias, padrão 180 s")
    ap.add_argument("--espera", type=int, default=10, help="Segundos com consumidores parados no teste de indisponibilidade")
    ap.add_argument("--limite-entrega", type=int, default=60, help="Segundos máximos aguardando entrega por teste")
    ap.add_argument("--saida", default="resultados", help="Diretório para os dados experimentais")
    args = ap.parse_args()
    padrao = {"smoke": 2, "roteamento": 100, "carga": 100, "indisponibilidade": 100}
    qtd = args.qtd if args.qtd is not None else padrao[args.cenario]
    if not 1 <= qtd <= MAX_POR_CENARIO:
        ap.error(f"--qtd deve estar entre 1 e {MAX_POR_CENARIO}")
    if not 1 <= args.repeticoes <= MAX_REPETICOES:
        ap.error(f"--repeticoes deve estar entre 1 e {MAX_REPETICOES}")
    if not 1 <= args.inicio_repeticao <= MAX_REPETICOES or args.inicio_repeticao + args.repeticoes - 1 > MAX_REPETICOES:
        ap.error("Intervalo de repetições deve estar entre 1 e 5")
    if not 60 <= args.pausa_repeticoes <= 600:
        ap.error("--pausa-repeticoes deve estar entre 60 e 600 segundos")
    if not 20 <= args.aguardar_fila <= 600:
        ap.error("--aguardar-fila deve estar entre 20 e 600 segundos")
    if args.cenario == "smoke" and (qtd != 2 or args.repeticoes != 1):
        ap.error("smoke deve ter --qtd 2 e --repeticoes 1")
    if not 0 <= args.espera <= 60 or not 10 <= args.limite_entrega <= 180:
        ap.error("--espera entre 0-60 e --limite-entrega entre 10-180")
    invalidos = 10 if args.cenario == "roteamento" else 0
    total = (qtd + invalidos) * args.repeticoes
    if total > MAX_TOTAL_EVENTOS_POR_EXECUCAO:
        ap.error("Limite de segurança de eventos atingido")
    # Sem perguntas interativas: imprime previamente o teto de custo da publicação.
    print(f"Planejados: {total} eventos (custo indicativo da publicação no Classic: US$ {total/1e6:.5f}; outros serviços não incluídos).", flush=True)
    sessao = boto3.Session(region_name=REGIAO)
    sts, events, sqs = (sessao.client(n, config=CONFIG) for n in ("sts", "events", "sqs"))
    saida = Path(args.saida)
    saida.mkdir(parents=True, exist_ok=True)
    urls = preflight(sts, events, sqs, args.aguardar_fila)
    for rep in range(args.inicio_repeticao, args.inicio_repeticao + args.repeticoes):
        # Os testes só prosseguem com filas vazias conforme contadores aproximados.
        m = executar_um("outage" if args.cenario == "indisponibilidade" else args.cenario,
                       rep, qtd, invalidos, args.espera, args.limite_entrega, events, sqs, urls, saida)
        if m["faltantes"] or m["fora_da_rota"] or m["erros_consumidor"] or m["erros_exclusao"]:
            print("Interrompendo execuções adicionais para não contaminar amostras.", file=sys.stderr)
            return 2
        if rep != args.inicio_repeticao + args.repeticoes - 1:
            print(f"[SQS] Pausa de estabilização de {args.pausa_repeticoes} segundos entre repetições.", flush=True)
            time.sleep(args.pausa_repeticoes)
            aguardar_filas_vazias(sqs, urls, limite_s=args.aguardar_fila)
    print(f"\nArquivos em: {saida.resolve()}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrompido pelo usuário. Verifique o SQS antes de nova execução.", file=sys.stderr)
        sys.exit(130)
    except (RuntimeError, AssertionError, ClientError, BotoCoreError) as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        sys.exit(1)
