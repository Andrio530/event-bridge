#!/usr/bin/env python3
"""Consulta demonstrativa de uma fila sem exclusão de mensagens por padrão.
A coleta válida do artigo usa consumidores integrados ao experimentos.py.
"""
import argparse
import json
import boto3

p=argparse.ArgumentParser()
p.add_argument('--fila',choices=['pedidos-normais','pedidos-prioritario'],required=True)
p.add_argument('--deletar',action='store_true',help='Confirma exclusão das mensagens encontradas')
a=p.parse_args()
sqs=boto3.client('sqs',region_name='us-east-2')
url=sqs.get_queue_url(QueueName=a.fila)['QueueUrl']
r=sqs.receive_message(QueueUrl=url,MaxNumberOfMessages=10,WaitTimeSeconds=2)
for m in r.get('Messages',[]):
    corpo=json.loads(m['Body'])
    print(json.dumps(corpo,ensure_ascii=False,indent=2))
    if a.deletar:
        sqs.delete_message(QueueUrl=url,ReceiptHandle=m['ReceiptHandle'])
print('Recebidas nesta consulta:',len(r.get('Messages',[])))
