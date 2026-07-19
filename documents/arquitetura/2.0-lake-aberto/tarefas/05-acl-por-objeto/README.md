# 05 · ACL por objeto — autorização das fontes no Postgres

> **Capturar, junto com a extração, QUEM tem acesso a CADA objeto extraído** — e
> persistir isso num **Postgres**. Para cada conector, além do dado, extraímos a
> **ACL da fonte**: qual objeto foi extraído, quais principais (usuário / grupo /
> papel / time / profile) têm acesso e em que nível.
>
> Conectores no escopo: **BigQuery, Confluence, HubSpot, Jira, Salesforce**.

Legenda: 🛑 a fazer · 🔥 **urgente**

---

## Por quê (urgente)

O dado das fontes vira **grafo + RAG** e é servido a usuários e LLMs. Se ingerimos
sem trazer a autorização da origem, a plataforma **vaza dado que o usuário não podia
ver na fonte** (um card do Jira restrito, um espaço privado no Confluence, um objeto
do Salesforce fora do sharing do usuário). A recuperação **precisa ser
permission-aware**, e a única forma é ter a ACL da fonte materializada ao lado do
dado. Sem isso, não dá pra abrir a plataforma pra cliente enterprise regulado.

**A ACL é metadado de extração, não conteúdo** — mora no **Postgres** (o mesmo
catálogo que já usamos), não no lake nem no grafo. O grafo/RAG **consulta** o
Postgres na hora de montar contexto pra filtrar por quem pergunta.

## Modelo de dados (Postgres) — proposta

Genérico, um esquema pros 5 conectores (evita N tabelas por sistema):

```sql
-- objeto extraído (o "recurso" da fonte)
source_object(
  id             uuid pk,
  source_system  text,           -- 'bigquery' | 'confluence' | 'hubspot' | 'jira' | 'salesforce'
  connection_id  text,           -- qual conexão/tenant do cliente
  object_type    text,           -- 'dataset'|'table'|'space'|'page'|'record'|'issue'|'project'|...
  object_id      text,           -- id nativo na fonte
  object_name    text,
  extracted_at   timestamptz
)

-- principal (quem pode ter acesso)
source_principal(
  id             uuid pk,
  source_system  text,
  connection_id  text,
  principal_type text,           -- 'user'|'group'|'role'|'team'|'profile'|'permission_set'|'service_account'
  principal_id   text,           -- id nativo
  principal_name text,
  email          text            -- p/ resolver contra o usuário logado na plataforma
)

-- a ACL em si (N:M objeto ↔ principal)
object_acl(
  source_object_id  uuid  references source_object,
  principal_id      uuid  references source_principal,
  access_level      text,        -- 'read'|'write'|'admin'|'owner'
  grant_type        text,        -- 'direct'|'group'|'role'|'sharing_rule'|'inherited'
  granted_via       text,        -- ex.: nome da role/sharing rule/grupo que deu o acesso
  extracted_at      timestamptz,
  primary key (source_object_id, principal_id, access_level)
)
```

> Chave de resolução: `source_principal.email` → usuário da plataforma. Grupos/roles/
> times exigem também trazer a **membership** (quem está em cada grupo) — modelar como
> `principal_membership(group_id, member_principal_id)` se o filtro for por grupo.

## Onde mora a ACL em cada conector

| Conector | Objeto | Onde está a autorização | API/fonte |
|---|---|---|---|
| **BigQuery** | dataset / table / view | IAM policy por recurso (+ column-level via policy tags, row-level via authorized views) | `datasets.getIamPolicy`, `tables.getIamPolicy`; Data Catalog p/ policy tags |
| **Confluence** | space / page | space permissions + content restrictions (herança space→page) | `space permissions` API + `content/{id}/restriction` |
| **HubSpot** | record por objeto (contact, deal, …) | roles + **teams** + ownership (acesso por hierarquia de time / owner) | Users & Teams API; permission set do usuário |
| **Jira** | project / issue | permission scheme (projeto) + **issue security scheme** (nível de segurança por issue) | project roles, permission scheme, issue security level |
| **Salesforce** | object / record | profiles + permission sets (objeto/campo) + **OWD / role hierarchy / sharing rules** (record) | Describe, `UserRecordAccess`, Profile/PermissionSet, sharing rules |

> Dois níveis por sistema: **object-level** (o principal pode ver *aquele tipo* de
> objeto) e **record-level** (pode ver *aquela linha* específica — sharing do
> Salesforce, issue security do Jira, restrictions do Confluence). Trazer os dois; o
> record-level é o que realmente evita vazamento.

## Tarefas

- 🔥 🛑 **Esquema no Postgres** — criar `source_object` / `source_principal` /
  `object_acl` (+ membership se necessário) e a migração.
- 🔥 🛑 **Extrator de ACL por conector** (BigQuery, Confluence, HubSpot, Jira,
  Salesforce) — roda junto/depois da extração do dado, popula as 3 tabelas.
- 🔥 🛑 **Normalização de principal** — mapear id nativo → `email` → usuário da
  plataforma; trazer membership de grupos/roles/times.
- 🛑 **Modelar object-level vs record-level** por sistema (o record-level é o
  crítico: sharing SF, issue security Jira, restrictions Confluence).
- 🛑 **Ponto de enforcement** — definir como o grafo/RAG consulta `object_acl` na
  montagem de contexto (filtro por quem pergunta) antes de servir. *(pode virar
  tarefa própria depois — aqui garantir que o dado existe pra isso.)*
- 🛑 **Freshness** — reextrair a ACL a cada run (permissões mudam); estratégia de
  diff/idempotência por objeto.

## Dependências

- **Depende da 02 (conectores)** — a ACL é extraída no mesmo run do conector, então
  cada conector no escopo precisa existir/estar estável.
- **Alimenta o enforcement do grafo/RAG** (permission-aware retrieval) — fora do
  escopo desta tarefa produzir o dado; a filtragem em si é consumo downstream.
- **Postgres** já existe (catálogo do starter) — reaproveitar, não subir infra nova.
