sequenceDiagram
    autonumber

    participant FE as Front-end
    participant BE as Back-end
    participant OCRMS as Microserviço OCR
    participant Bridge as Bridge APIs
    participant LLMMS as Microserviço LLM
    participant Mongo as MongoDB

    FE->>BE: POST /analysis (arquivo)

    BE->>OCRMS: Envia arquivo
    OCRMS->>Bridge: Chama endpoint OCR
    Bridge-->>OCRMS: Texto extraído (OCR)
    OCRMS-->>BE: Resultado OCR

    BE->>LLMMS: Texto OCR + instruções
    LLMMS->>Bridge: Chama endpoint LLM
    Bridge-->>LLMMS: Resultado estruturado
    LLMMS-->>BE: Resultado da análise

    BE->>Mongo: Salva análise

    BE-->>FE: Ticket de processamento

    loop Polling
        FE->>BE: GET /analysis/{ticket}
        alt Processando
            BE-->>FE: Status = PROCESSING
        else Finalizado
            BE->>Mongo: Busca resultado
            Mongo-->>BE: Dados da análise
            BE-->>FE: Status = COMPLETED + Resultado
        end
    end