# 04 · Data quality — gate na ingestão

> Checks de qualidade **antes do RAW**: staging → passou vira RAW, falhou vai pra
> **quarentena**. Impede que dado ruim/bot vaze pro grafo.
>
> Base: protótipo com Great Expectations (o A/B mostrava o gate zerando 146 registros ruins
> + 61 bots que vazariam pro grafo — recall 100%). 🛑 O folder de experimento **não está no
> repo no momento** (recriar).

Legenda: 🛑 a fazer

---

## Ideia

- **Checks por tabela na config do conector** (esquema `config → suite`).
- **Staging antes do RAW**: valida o batch → separa o que passa do que falha.
- Cobria os ~10 tipos de check (nulos, ranges, unicidade, formato de email/cpf, etc.).

## Tarefas

- 🛑 **Recriar o folder de experimento** (era o `09-data-quality-great-expectations`) e o A/B.
- 🛑 **Definir o esquema `config → suite`** — como o conector declara os checks.
- 🛑 **Wiring num conector piloto**: staging → RAW | quarentena, com o gate rodando no flow.
- 🛑 **A/B real** quando o eval rodar (medir recall/precision do gate em dado de verdade).

> Entra **depois** que a ingestão ([tarefa 02](../02-conectores-dlt-connectorx/)) estabilizar
> — o gate fica entre a extração e a escrita no RAW.
