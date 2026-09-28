```mermaid
flowchart TD
    A["1. Lê fornecedores do RAW<br/>(csv / landing path)"] --> B["2. Calcula HASH SHA-256<br/>dos atributos (endereço, status, dados bancários)"]
    B --> C{"3. A tabela Silver<br/>já existe?"}
    
    C -->|Não| D["4. Cria primeira versão (Bootstrap)<br/>• is_current = true<br/>• start_date = data_atual<br/>• end_date = null"]
    C -->|Sim| E["5. Compara com registros atuais<br/>(is_current = true)"]
    
    E --> F{"6. Detecta alterações<br/>pelo HASH?"}
    
    F -->|Não| G["Nenhuma ação necessária<br/>(Manter registro atual)"]
    F -->|Sim| H["Duplica chaves de Join<br/>(Null Key Trick / Staging)"]
    
    H --> I["7. Executa Delta MERGE INTO:<br/>Inativa a versão antiga<br/>• is_current = false<br/>• end_date = data_atual"]
    I --> J["8. Insere a nova versão do registro<br/>• is_current = true<br/>• start_date = data_atual<br/>• end_date = null"]
    
    D --> K[("9. Mantém histórico preservado<br/>no Delta Lake (SCD Tipo 2)")]
    G --> K
    J --> K