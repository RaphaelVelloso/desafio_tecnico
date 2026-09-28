flowchart TD
    A["Entrada de Eventos do Stream<br/>(sensores_iot.json)"] --> B["1. Definição do Watermark<br/>(ex: Tolerância de 2 Horas)"]
    
    B --> C{"2. Evento chegou dentro<br/>da janela aceita?"}
    
    C -->|Não / Atraso Excessivo| D["Descartado Automaticamente<br/>(Evita estouro da memória de estado)"]
    
    C -->|Sim| E{"3. Combinação única<br/>[sensor_id + timestamp]<br/>já existe no estado?"}
    
    E -->|Sim / Duplicado| F["Descartado<br/>(Retransmissão de Rede)"]
    
    E -->|Não / Novo Registro| G["1. Grava no Estado do Spark<br/>2. Escreve na Silver (Delta Lake)"]