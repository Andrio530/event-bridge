"""Testes offline: nao fazem nenhuma chamada AWS."""
import csv
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'dados'/'brutos'

def linhas(arquivo):
    with (DATA/arquivo).open(newline='',encoding='utf-8') as f:
        return list(csv.DictReader(f))

class TestesDadosAWS(unittest.TestCase):
    def test_execucoes_e_totais(self):
        resumo=linhas('resumo.csv')
        self.assertEqual(len(resumo),18)
        self.assertEqual(sum(int(x['eventos_aceitos']) for x in resumo),8212)
        self.assertEqual(sum(int(x['esperados_na_fila']) for x in resumo),8202)
        self.assertEqual(sum(int(x['entregues_corretamente_unicos']) for x in resumo),8202)
        self.assertEqual(set(x['cenario'] for x in resumo),{'smoke','roteamento','carga','outage'})
        self.assertEqual(sum(int(x['faltantes']) for x in resumo),0)
        self.assertEqual(sum(int(x['fora_da_rota']) for x in resumo),0)
        self.assertEqual(sum(int(x['duplicacoes']) for x in resumo),0)

    def test_envios_e_recebimentos_por_execucao(self):
        vistos=set()
        for r in linhas('resumo.csv'):
            enviados=linhas('envios_'+r['run_id']+'.csv')
            recebidos=linhas('recebimentos_'+r['run_id']+'.csv')
            self.assertEqual(len(enviados),int(r['eventos_planejados']))
            self.assertEqual(len(recebidos),int(r['entregues_corretamente_unicos']))
            ids={x['evento_id'] for x in recebidos}
            self.assertEqual(len(ids),len(recebidos))
            for x in recebidos:
                self.assertNotIn(x['evento_id'],vistos)
                vistos.add(x['evento_id'])
                self.assertEqual(x['delete_sucesso'],'True')
                self.assertGreaterEqual(float(x['latencia_ms']),0)
        self.assertEqual(len(vistos),8202)

    def test_indisponibilidade(self):
        o=[r for r in linhas('resumo.csv') if r['cenario']=='outage']
        self.assertEqual(len(o),1)
        self.assertEqual(int(o[0]['entregues_corretamente_unicos']),100)
        self.assertAlmostEqual(float(o[0]['tempo_recuperacao_s']),11.679,places=3)
        self.assertGreaterEqual(len(linhas('backlog_'+o[0]['run_id']+'.csv')),2)

if __name__=='__main__': unittest.main()
