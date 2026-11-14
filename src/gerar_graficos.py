#!/usr/bin/env python3
"""Recalcula métricas e gera figuras a partir dos CSVs medidos na AWS.

Uso: python src/gerar_graficos.py
Nao acessa a AWS e nao modifica dados brutos.
"""
from pathlib import Path
import argparse
import csv
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

ROOT = Path(__file__).resolve().parents[1]

def salvar(fig, destino):
    fig.savefig(destino, dpi=220, bbox_inches='tight', facecolor='white')
    plt.close(fig)


def principal(dados, figuras, processados):
    figuras.mkdir(parents=True, exist_ok=True)
    processados.mkdir(parents=True, exist_ok=True)
    resumo = pd.read_csv(dados/'resumo.csv')
    assert len(resumo) == 18, f'Esperadas 18 execuções, encontradas {len(resumo)}'
    assert {'smoke','roteamento','carga','outage'} == set(resumo.cenario)
    assert resumo['eventos_aceitos'].sum() == 8212
    assert resumo['entregues_corretamente_unicos'].sum() == 8202
    assert resumo[['faltantes','fora_da_rota','duplicacoes','erros_consumidor','erros_exclusao']].fillna(0).sum().sum() == 0
    recebidos, envios = [], []
    for row in resumo.itertuples(index=False):
        a = pd.read_csv(dados/f'envios_{row.run_id}.csv')
        b = pd.read_csv(dados/f'recebimentos_{row.run_id}.csv')
        assert len(a) == row.eventos_planejados, row.run_id
        assert len(b) == row.entregues_corretamente_unicos, row.run_id
        assert b['evento_id'].is_unique, row.run_id
        assert b['delete_sucesso'].all(), row.run_id
        assert b['latencia_ms'].notna().all(), row.run_id
        if row.cenario == 'carga':
            b = b.assign(carga=row.eventos_planejados)
            recebidos.append(b)
        envios.append(a)
    recebidos = pd.concat(recebidos, ignore_index=True)
    envios = pd.concat(envios, ignore_index=True)
    assert len(recebidos) == 8000
    assert envios['evento_id'].is_unique

    cargas = resumo[resumo.cenario == 'carga'].copy()
    assert sorted(cargas.groupby('eventos_planejados').size().tolist()) == [5,5,5]
    estatisticas=[]
    for n, bloco in cargas.groupby('eventos_planejados'):
        r = recebidos[recebidos.carga==n]
        estatisticas.append({
            'carga_por_execucao': int(n), 'repeticoes': len(bloco),
            'publicados': int(bloco.eventos_aceitos.sum()),
            'entregues': int(bloco.entregues_corretamente_unicos.sum()),
            'latencia_media_s': r.latencia_ms.mean()/1000,
            'latencia_mediana_s': r.latencia_ms.median()/1000,
            'latencia_p95_global_s': r.latencia_ms.quantile(.95)/1000,
            'latencia_p99_global_s': r.latencia_ms.quantile(.99)/1000,
            'desvio_padrao_medias_s': bloco.latencia_media_ms.std(ddof=1)/1000,
            'vazao_media_eventos_s': bloco.vazao_entrega_eventos_s.mean(),
            'vazao_minima_eventos_s': bloco.vazao_entrega_eventos_s.min(),
            'vazao_maxima_eventos_s': bloco.vazao_entrega_eventos_s.max(),
            'publicacao_min_s': bloco.duracao_publicacao_s.min(),
            'publicacao_max_s': bloco.duracao_publicacao_s.max(),
        })
    pd.DataFrame(estatisticas).sort_values('carga_por_execucao').to_csv(processados/'estatisticas_por_carga.csv',index=False,float_format='%.5f')
    cargas.to_csv(processados/'metricas_por_execucao.csv',index=False)
    resumo.to_csv(processados/'resumo_consolidado.csv',index=False)

    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.titlesize':12,
        'axes.labelsize':10,'figure.dpi':120,'savefig.dpi':220})
    # arquitetura real, sem sugerir que SQS e um barramento
    fig,ax=plt.subplots(figsize=(10,4.4)); ax.set(xlim=(0,10),ylim=(0,5.6));ax.axis('off')
    def node(x,y,w,h,t,sub='',fill='0.95'):
        ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=.07,rounding_size=.12',
            linewidth=1.25,edgecolor='0.3',facecolor=fill))
        ax.text(x+w/2,y+h*(.62 if sub else .5),t,ha='center',va='center',weight='bold',fontsize=10)
        if sub:ax.text(x+w/2,y+h*.29,sub,ha='center',va='center',fontsize=9)
    def edge(x1,y1,x2,y2):ax.add_patch(FancyArrowPatch((x1,y1),(x2,y2),arrowstyle='-|>',mutation_scale=13,lw=1.25,color='0.28'))
    node(3.7,4.45,2.6,.78,'Produtor Python','PutEvents / JSON')
    node(3.65,3.01,2.7,.78,'EventBridge Classic','event bus: pedidos','0.91')
    node(.52,1.76,3.35,.59,'Regra: tipo = normal')
    node(6.13,1.76,3.35,.59,'Regra: tipo = prioritario')
    node(.52,.58,3.35,.63,'SQS Standard','pedidos-normais')
    node(6.13,.58,3.35,.63,'SQS Standard','pedidos-prioritario')
    edge(5,4.45,5,3.82);edge(4.32,3.01,2.19,2.38);edge(5.68,3.01,7.8,2.38)
    edge(2.19,1.76,2.19,1.24);edge(7.8,1.76,7.8,1.24)
    ax.text(2.2,.27,'Consumidor Python A',ha='center',fontsize=9)
    ax.text(7.8,.27,'Consumidor Python B',ha='center',fontsize=9)
    salvar(fig, figuras/'arquitetura.png')
    
    # distribuição pooled por volume, percentis calculados dos eventos
    fig,ax=plt.subplots(figsize=(8.4,4.6))
    grupos=[(recebidos[recebidos.carga==n].latencia_ms/1000).values for n in [100,500,1000]]
    ax.boxplot(grupos,tick_labels=['100','500','1.000'],whis=[5,95],showfliers=False,medianprops={'linewidth':1.8})
    for i,g in enumerate(grupos): ax.plot(i+1,np.quantile(g,.95),marker='D',color='black',mfc='white',markersize=6,label='p95 global' if i==0 else None)
    ax.set(xlabel='Eventos por execução (5 repetições)',ylabel='Latência ponta a ponta (s)',title='Distribuição das latências individuais')
    ax.legend(loc='upper left');ax.grid(axis='y',alpha=.25);fig.tight_layout();salvar(fig,figuras/'distribuicao_latencia.png')
    # por repetição
    fig,ax=plt.subplots(figsize=(8.4,4.4))
    for n,g in cargas.groupby('eventos_planejados'):
        g=g.sort_values('repeticao')
        ax.plot(g.repeticao, g.latencia_media_ms/1000,marker='o',label=f'{n:,}'.replace(',','.')+' eventos')
    ax.set(xlabel='Repetição',ylabel='Latência média ponta a ponta (s)',title='Variabilidade entre repetições')
    ax.set_xticks([1,2,3,4,5]);ax.legend();ax.grid(alpha=.24);fig.tight_layout();salvar(fig,figuras/'variabilidade_repeticoes.png')
    # vazão por repetição
    fig,ax=plt.subplots(figsize=(8.4,4.4))
    for n,g in cargas.groupby('eventos_planejados'):
        g=g.sort_values('repeticao')
        ax.plot(g.repeticao,g.vazao_entrega_eventos_s,marker='o',label=f'{n:,}'.replace(',','.')+' eventos')
    ax.set(xlabel='Repetição',ylabel='Vazão observada (eventos/s)',title='Vazão ponta a ponta por execução')
    ax.set_xticks([1,2,3,4,5]);ax.legend();ax.grid(alpha=.24);fig.tight_layout();salvar(fig,figuras/'vazao_repeticoes.png')
    # tempo de publicação x latência
    fig,ax=plt.subplots(figsize=(8.1,4.4))
    for n,g in cargas.groupby('eventos_planejados'):
        ax.scatter(g.duracao_publicacao_s,g.latencia_media_ms/1000,s=44,label=f'{n:,}'.replace(',','.')+' eventos')
    ax.set(xlabel='Duração da publicação (s)',ylabel='Latência média ponta a ponta (s)',title='Duração da publicação versus latência')
    ax.legend();ax.grid(alpha=.24);fig.tight_layout();salvar(fig,figuras/'publicacao_vs_latencia.png')

    # outage: contadores aproximados; nao inferir perdas a partir de snapshots
    outage = resumo[resumo.cenario=='outage'].iloc[0]
    backlog = pd.read_csv(dados/f'backlog_{outage.run_id}.csv')
    outage_rec = pd.read_csv(dados/f'recebimentos_{outage.run_id}.csv')
    outage_rec['recebido_utc'] = pd.to_datetime(outage_rec['recebido_utc'],utc=True)
    outage_rec = outage_rec.sort_values('recebido_utc')
    backlog['aprox_total'] = backlog['disponiveis_aprox'] + backlog['invisiveis_aprox']
    fig,ax=plt.subplots(figsize=(8.6,4.6))
    for fila, grupo in backlog.groupby('fila'):
        ax.plot(grupo.segundos_desde_inicio, grupo.aprox_total, 'o-',lw=1,markersize=4,label=fila)
    ax.axvline(10.313,ls='--',lw=1,color='0.4',label='Amostra anterior ao reinício')
    ax.set(xlabel='Segundos desde o início do experimento',ylabel='Mensagens (contadores aproximados)',title='Amostragem da profundidade das filas')
    ax.legend(fontsize=8);ax.grid(alpha=.24);fig.tight_layout();salvar(fig,figuras/'backlog_aproximado.png')

    # dados de recebimento são evidência mais sólida que contadores aproximados
    fig,ax=plt.subplots(figsize=(8.4,4.3))
    t0 = outage_rec.recebido_utc.iloc[0]
    t = (outage_rec.recebido_utc-t0).dt.total_seconds().values
    ax.step(np.r_[0,t],np.r_[0,np.arange(1,len(t)+1)],where='post',lw=1.7)
    ax.set(xlabel='Segundos desde o primeiro recebimento após reinício',ylabel='Eventos únicos recebidos (acumulado)',title='Recuperação dos eventos após indisponibilidade')
    ax.set_ylim(0,105);ax.set_xlim(0,max(t)+.3);ax.grid(alpha=.24);fig.tight_layout();salvar(fig,figuras/'recuperacao_acumulada.png')
    print('Carga:')
    print(pd.DataFrame(estatisticas).round(3).to_string(index=False))
    print('\nOutage: ',outage[['eventos_aceitos','entregues_corretamente_unicos','tempo_recuperacao_s','backlog_antes_reinicio_aprox','latencia_media_ms','latencia_p95_ms']].to_dict())
    print('Gráficos:',len(list(figuras.glob('*.png'))))

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--dados',type=Path,default=ROOT/'dados'/'brutos')
    p.add_argument('--figuras',type=Path,default=ROOT/'figuras')
    p.add_argument('--processados',type=Path,default=ROOT/'dados'/'processados')
    a=p.parse_args()
    principal(a.dados,a.figuras,a.processados)
