#!/usr/bin/env python3
"""Demonstração de publicação: dois eventos, um por regra, sem custo de servidores.
Este exemplo não foi usado para as estatísticas finais; use experimentos.py.
"""
import json
import uuid
from datetime import datetime, timezone
import boto3

REGIAO = "us-east-2"
BARRAMENTO = "pedidos"
client = boto3.client("events", region_name=REGIAO)
entries = []
for tipo in ["normal", "prioritario"]:
    entries.append({
        "Source": "sbc.pedidos",
        "DetailType": "PedidoCriado",
        "EventBusName": BARRAMENTO,
        "Detail": json.dumps({
            "evento_id": str(uuid.uuid4()),
            "tipo": tipo,
            "valor": 100,
            "instante_envio": datetime.now(timezone.utc).isoformat(),
        }),
    })
r = client.put_events(Entries=entries)
if r.get("FailedEntryCount", 0):
    raise RuntimeError(r)
print("Eventos aceitos:", len(r.get("Entries", [])))
print("Atencao: aceite na API nao garante entrega ao destino.")
