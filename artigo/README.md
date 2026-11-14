# Artigo modelo SBC

`artigo_eventbridge_sbc.tex` foi preparado com a estrutura SBC (`abstract`, `resumo`, seções, figuras, bibliografia) e PDF compilado localmente com um estilo **compatível para prévia** (`sbc-template-compat.sty`). **O arquivo compat não substitui o pacote oficial `sbc-template.sty`.** Para submissão, importe o `.tex` e a pasta `figuras/` para um projeto SBC oficial no Overleaf, mantendo os caminhos das figuras, e confira a paginação. Os gráficos são criados por `python src/gerar_graficos.py`.

A autoria principal está como Andrio Epping; revise o nome e acrescente coautores apenas se forem confirmados. O artigo usa os resultados reais de 14 de novembro de 2026.

Para compilar diretamente a partir desta pasta do repositório:

```bash
cd artigo
pdflatex -interaction=nonstopmode artigo_eventbridge_sbc.tex
pdflatex -interaction=nonstopmode artigo_eventbridge_sbc.tex
```

Modelo SBC disponibilizado pela própria Sociedade Brasileira de Computação:
https://www.overleaf.com/latex/templates/sbc-conferences-template-updated-sbc-template-dot-sty-v2017/pyhttxftxjqn

O PDF da prévia está com 8 páginas em A4 e foi inspecionado após a compilação.
