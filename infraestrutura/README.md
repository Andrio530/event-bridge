# Infraestrutura AWS real — EventBridge Classic + SQS Standard

Os testes originais foram executados manualmente com um EventBridge Classic (`pedidos`), duas regras e duas filas Standard em `us-east-2`. Os destinos receberam acesso **pela policy da fila**, com `Principal.Service=events.amazonaws.com` e `aws:SourceArn` correspondente à regra. **Os destinos não devem ter `RoleArn`** neste experimento.

Arquivos:

- `padrao_normal.json` e `padrao_prioritario.json`: correspondência por conteúdo de cada regra.
- `cloudformation.yaml`: **alternativa declarativa** para criar infraestrutura equivalente em uma **conta nova ou região sem recursos com esses nomes**. Esse template foi revisado e validado sintaticamente localmente, mas não foi implantado neste ambiente; o protótipo original foi configurado manualmente.

## ATENÇÃO: custos e conflitos

**Não execute `deploy` na conta original** se o barramento ou as filas já existirem. Os nomes são fixos e ocorrerão conflitos; criar recursos adicionais pode gerar cobrança. Uma publicação pequena costuma custar pouco, mas o custo total da conta não é garantido. Verifique seu AWS Budget.

Validação sem implantação (pode exigir AWS CLI e credenciais autorizadas):

```bash
aws cloudformation validate-template \
  --template-body file://infraestrutura/cloudformation.yaml \
  --region us-east-2
```

Na **conta isolada**, após entender possíveis custos, a implantação opcional seria:

```bash
aws cloudformation deploy \
  --stack-name sbc-eventbridge-estudo \
  --template-file infraestrutura/cloudformation.yaml \
  --region us-east-2
```

O template não ativa DLQ, arquivo/replay, log adicional, EC2 ou bancos de dados. Antes de excluir uma stack, verifique se ela contém dados necessários, pois a exclusão de filas remove mensagens nelas armazenadas.

## Referência oficial

- https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-resource-based.html
- https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-events-rule.html
